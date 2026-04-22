package watcher

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

// RateLimitedError is returned when Wikidata responds with HTTP 429
// or a maxlag error.
type RateLimitedError struct {
	RetryAfter time.Duration
	Maxlag     bool // true if caused by maxlag, false if HTTP 429
}

func (e *RateLimitedError) Error() string {
	return fmt.Sprintf("rate limited, retry after %v", e.RetryAfter)
}

// WikidataEntity represents relevant data extracted from a Wikidata entity.
type WikidataEntity struct {
	ID       string
	Mappings []string // flat array of "P<id>:<value>" entries
}

// WikidataClient fetches entity data from Wikidata.
// It tracks consecutive maxlag errors and applies exponential backoff
// (capped at maxlagBackoffMax) to avoid hammering a struggling server.
// The maxlag query parameter starts at wikimediaMaxlag and increases by
// one second per minute of cumulative maxlag wait, capped at maxlagParamMax.
type WikidataClient struct {
	client          *http.Client
	baseURL         string
	maxlagBackoff   time.Duration // current backoff; 0 when healthy
	maxlagWaitTotal time.Duration // cumulative time spent waiting on maxlag
}

const (
	maxlagBackoffInit = 10 * time.Second // initial backoff after first maxlag
	maxlagBackoffMax  = 5 * time.Minute  // cap for exponential backoff
	maxlagParamMax    = 30.0             // upper bound for dynamic maxlag parameter
)

// maxlagParam returns the current maxlag query-parameter value as a string.
// It starts at wikimediaMaxlag and increases by one per minute of cumulative
// maxlag wait, capped at maxlagParamMax.
func (c *WikidataClient) maxlagParam() string {
	return fmt.Sprintf("%d", int(min(wikimediaMaxlag+c.maxlagWaitTotal.Minutes(), maxlagParamMax)))
}

// NewWikidataClient creates a new Wikidata API client.
// The client's transport should include throttling (ThrottleTransport)
// for production use. Tests can pass a plain httptest server client.
func NewWikidataClient(client *http.Client) *WikidataClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &WikidataClient{
		client:  client,
		baseURL: "https://www.wikidata.org",
	}
}

// EntityResult holds the raw JSON for a single entity from a batch fetch,
// or marks it as missing (deleted on Wikidata).
type EntityResult struct {
	Data    []byte // Raw JSON for the single entity object
	Missing bool   // True if the entity was deleted/missing on Wikidata
}

// FetchEntitiesRaw fetches raw JSON for one or more Wikidata entities using
// the wbgetentities API. Returns a map from QID to EntityResult. Missing
// entities (deleted on Wikidata) are returned with Missing=true rather than
// as errors. The qids slice must not exceed 50 entries (Wikidata API limit).
func (c *WikidataClient) FetchEntitiesRaw(qids []string) (map[string]EntityResult, error) {
	if len(qids) == 0 {
		return nil, nil
	}
	if len(qids) > 50 {
		return nil, fmt.Errorf("wbgetentities: batch size %d exceeds limit of 50", len(qids))
	}

	apiURL := fmt.Sprintf("%s/w/api.php?action=wbgetentities&format=json&redirects=no&maxlag=%s&props=claims&ids=%s",
		c.baseURL, c.maxlagParam(), strings.Join(qids, "%7C"))

	req, err := newRequest("GET", apiURL)
	if err != nil {
		return nil, fmt.Errorf("fetch entities: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch entities: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := 30 * time.Second // default backoff
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		return nil, &RateLimitedError{RetryAfter: retryAfter}
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch entities: status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 50<<20)) // 50 MB limit for batch
	if err != nil {
		return nil, fmt.Errorf("read entities response: %w", err)
	}

	// Check for maxlag error (returned as HTTP 200 with JSON error body).
	var apiErr struct {
		Error *struct {
			Code string  `json:"code"`
			Lag  float64 `json:"lag"`
		} `json:"error"`
	}

	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error != nil && apiErr.Error.Code == "maxlag" {
		retryAfter := 5 * time.Second // default per Wikidata recommendation
		if ra := resp.Header.Get("Retry-After"); ra != "" {
			if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		// Exponential backoff for consecutive maxlag errors.
		if c.maxlagBackoff == 0 {
			c.maxlagBackoff = maxlagBackoffInit
		} else {
			c.maxlagBackoff *= 2
		}
		c.maxlagBackoff = min(c.maxlagBackoff, maxlagBackoffMax)
		retryAfter = max(retryAfter, c.maxlagBackoff)
		c.maxlagWaitTotal += retryAfter
		slog.Warn("maxlag backoff", "delay", retryAfter, "lag", apiErr.Error.Lag, "next_backoff", min(c.maxlagBackoff*2, maxlagBackoffMax), "next_maxlag", c.maxlagParam())
		return nil, &RateLimitedError{RetryAfter: retryAfter, Maxlag: true}
	}

	// Successful response — reset maxlag backoff and cumulative wait.
	c.maxlagBackoff = 0
	c.maxlagWaitTotal = 0

	// Parse the top-level structure. Check for non-maxlag API errors first:
	// the response may be valid JSON with an "error" key but no "entities"
	// key (e.g. readonly mode, internal errors). Without this check, a
	// missing "entities" map causes every QID to be treated as deleted.
	var envelope struct {
		Entities map[string]json.RawMessage `json:"entities"`
		Error    *struct {
			Code string `json:"code"`
			Info string `json:"info"`
		} `json:"error"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, fmt.Errorf("parse entities response: %w", err)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("wbgetentities API error: %s: %s", envelope.Error.Code, envelope.Error.Info)
	}
	if len(envelope.Entities) == 0 {
		return nil, fmt.Errorf("wbgetentities returned no entities")
	}

	results := make(map[string]EntityResult, len(envelope.Entities))
	for qid, raw := range envelope.Entities {
		// Check for the "missing" key that wbgetentities uses for deleted entities.
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) == nil {
			if _, hasMissing := fields["missing"]; hasMissing {
				results[qid] = EntityResult{Missing: true}
				continue
			}
		}

		results[qid] = EntityResult{Data: raw}
	}

	return results, nil
}

// ParseEntityJSON parses a bare Wikidata entity JSON object and extracts all
// external ID claims in a single pass. Non-Q entities return (nil, nil) so
// property and lexeme lines are silently skipped.
func ParseEntityJSON(data []byte) (*WikidataEntity, error) {
	var raw struct {
		ID     string    `json:"id"`
		Claims claimsMap `json:"claims"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("parse entity: %w", err)
	}

	// Skip non-Q entities (properties, lexemes).
	if !strings.HasPrefix(raw.ID, "Q") {
		return nil, nil
	}

	externalIDs := extractExternalIDs(raw.Claims)

	return &WikidataEntity{
		ID:       raw.ID,
		Mappings: externalIDs,
	}, nil
}

// claimsMap is the parsed structure of Wikidata entity claims.
type claimsMap map[string][]struct {
	MainSnak struct {
		DataType  string `json:"datatype"`
		DataValue struct {
			Value json.RawMessage `json:"value"`
			Type  string          `json:"type"`
		} `json:"datavalue"`
	} `json:"mainsnak"`
	Rank string `json:"rank"`
}

// rankOrder maps Wikidata claim ranks to sort priority.
// Lower value = higher priority (preferred first, deprecated last).
// Normal (the default rank) is zero.
func rankOrder(rank string) int {
	switch rank {
	case "preferred":
		return -1
	case "normal", "":
		return 0
	case "deprecated":
		return 1
	default:
		return 0
	}
}

// extractExternalIDs collects all claims with datatype "external-id"
// and returns them as a deduplicated flat array of "P<id>:<value>" strings,
// sorted by property key, then rank (preferred > normal > deprecated),
// then value.
func extractExternalIDs(claims claimsMap) []string {
	type entry struct {
		propKey string // map key reference, no allocation
		rank    int
		value   string
	}
	entries := make([]entry, 0, len(claims))
	for propKey, propClaims := range claims {
		if len(propKey) < 2 || propKey[0] != 'P' {
			continue
		}
		if _, err := strconv.Atoi(propKey[1:]); err != nil {
			continue
		}
		for _, claim := range propClaims {
			if claim.MainSnak.DataType != "external-id" {
				continue
			}
			value := extractStringValue(claim.MainSnak.DataValue.Value)
			if value != "" {
				entries = append(entries, entry{
					propKey: propKey,
					rank:    rankOrder(claim.Rank),
					value:   value,
				})
			}
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].propKey != entries[j].propKey {
			return entries[i].propKey < entries[j].propKey
		}
		if entries[i].rank != entries[j].rank {
			return entries[i].rank < entries[j].rank
		}
		return entries[i].value < entries[j].value
	})
	// Duplicates (same propKey + value, different rank) are adjacent after
	// sorting, so dedup is a simple look-back. String concatenation is
	// deferred to here so only surviving entries allocate.
	result := make([]string, 0, len(entries))
	for i, e := range entries {
		if i > 0 && entries[i-1].propKey == e.propKey && entries[i-1].value == e.value {
			continue
		}
		result = append(result, e.propKey+":"+e.value)
	}
	return result
}

// extractStringValue extracts a string value from a Wikidata datavalue.
func extractStringValue(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return ""
}
