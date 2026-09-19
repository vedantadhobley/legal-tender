package reportmetadata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// Fetch captures one bounded query into a NEW directory. No directory is
// overwritten or resumed, and no bulk/current/graph pointer is touched.
func Fetch(ctx context.Context, directory string, request FetchRequest, key string) (FetchResult, error) {
	client := metadataClient()
	defer client.CloseIdleConnections()
	return fetch(ctx, directory, request, key, client, waitRequest)
}

func ValidateFetchRequest(r FetchRequest) error {
	if r.Contract != Contract || r.SchemaSHA256 != SwaggerSHA256 {
		return errors.New("unsupported metadata capture contract or schema")
	}
	if err := validateQuery(r.Endpoint, r.Query); err != nil {
		return err
	}
	l := r.Limits
	if l.Pages < 1 || l.Pages > MaxPages || l.Requests < 1 || l.Requests > 48 || l.AttemptsPerPage < 1 || l.AttemptsPerPage > 3 || l.SourceBytes < 1024 || l.SourceBytes > MaxCaptureBytes {
		return errors.New("invalid metadata capture budgets")
	}
	return nil
}

// ReadFetchRequest accepts only bounded, credential-free configuration.
func ReadFetchRequest(path string) (FetchRequest, error) {
	var r FetchRequest
	f, err := os.Open(path)
	if err != nil {
		return r, errors.New("cannot open metadata request")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return r, errors.New("metadata request must be a regular file")
	}
	b, err := io.ReadAll(io.LimitReader(f, (256<<10)+1))
	if err != nil || len(b) > 256<<10 {
		return r, errors.New("metadata request exceeds read budget")
	}
	if err := strictJSON(b, &r); err != nil {
		return r, err
	}
	return r, ValidateFetchRequest(r)
}

func fetch(ctx context.Context, directory string, r FetchRequest, key string, client *http.Client, wait func(context.Context, time.Duration) error) (result FetchResult, err error) {
	if err := ValidateFetchRequest(r); err != nil {
		return result, err
	}
	if !regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`).MatchString(key) {
		return result, errors.New("missing or invalid API credential")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := os.Mkdir(directory, 0700); err != nil {
		return result, errors.New("capture directory must be new and its parent must exist")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		return result, errors.New("cannot open new capture directory")
	}
	defer root.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	result = FetchResult{Version: FetchVersion, Request: r, StartedAt: stamp(), State: "failed", Reason: "interrupted", HeaderRepresentation: "go_http_values_sensitive_headers_omitted.v1", Attempts: []Attempt{}}
	if _, err := writeJSON(root, "request.json", r); err != nil {
		return result, err
	}
	// result.json is written only on return. Process disappearance without this
	// final record is never a success marker; per-attempt starts remain evidence.
	defer func() {
		result.FinishedAt = stamp()
		_, writeErr := writeJSON(root, "result.json", result)
		if writeErr != nil {
			err = writeErr
		}
	}()
	capture := Capture{Contract: Contract, SchemaSHA256: SwaggerSHA256, Endpoint: r.Endpoint, Query: r.Query, Pages: []PageCapture{}}
	stop := func(state, reason string) (FetchResult, error) {
		result.State = state
		result.Reason = reason
		return result, nil
	}
	for page := 1; page <= r.Limits.Pages; page++ {
		var selected *PageCapture
		for attempt := 1; attempt <= r.Limits.AttemptsPerPage; attempt++ {
			if ctx.Err() != nil {
				return stop("incomplete", "canceled_or_deadline")
			}
			if len(result.Attempts) >= r.Limits.Requests {
				return stop("incomplete", "request_budget")
			}
			if result.BytesRead >= r.Limits.SourceBytes {
				return stop("incomplete", "source_byte_budget")
			}
			if len(result.Attempts) > 0 {
				if wait(ctx, time.Second) != nil {
					return stop("incomplete", "canceled_or_deadline")
				}
			}
			a := Attempt{Sequence: len(result.Attempts) + 1, Page: page, URL: requestURL(r, page), StartedAt: stamp(), Outcome: "started", OmittedHeaders: []string{}}
			prefix := fmt.Sprintf("attempt-%03d", a.Sequence)
			if _, err := writeJSON(root, prefix+"-start.json", a); err != nil {
				return result, err
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.URL, nil)
			if err != nil {
				return result, errors.New("cannot construct metadata request")
			}
			req.Header.Set("X-Api-Key", key)
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Accept-Encoding", "identity")
			resp, transportErr := client.Do(req)
			var retry bool
			delay := time.Second
			if transportErr != nil {
				// Never retain error text: it may echo a URL or credential.
				if resp != nil && resp.Body != nil {
					resp.Body.Close()
				}
				a.Outcome = "transport_error"
				retry = true
			} else {
				a.Status = resp.StatusCode
				headers, body, spent, complete, outcome, omitted := readResponse(resp, r.Limits.SourceBytes-result.BytesRead, key)
				a.BytesRead = spent
				result.BytesRead += spent
				a.BodyComplete = complete
				a.Outcome = outcome
				a.OmittedHeaders = omitted
				if headers != nil {
					ref, err := writeBytes(root, prefix+".headers", headers)
					if err != nil {
						return result, err
					}
					a.Headers = &ref
				}
				if body != nil {
					ref, err := writeBytes(root, prefix+".body", body)
					if err != nil {
						return result, err
					}
					a.Body = &ref
				}
				if outcome == "response_received" {
					switch resp.StatusCode {
					case 200:
						pc := PageCapture{Page: page, ObservedAt: stamp(), TimeBasis: "client_clock", Body: *a.Body, Headers: *a.Headers}
						if validateHeaders(headers, pc) != nil {
							a.Outcome = "rejected_transport"
						} else {
							a.Outcome = "captured_response"
							selected = &pc
						}
					case 429, 500, 502, 503, 504:
						a.Outcome = "retryable_http_status"
						retry = true
						delay = retryDelay(resp.Header.Get("Retry-After"), time.Now())
						a.RetryAfterSeconds = int64((delay + time.Second - 1) / time.Second)
					default:
						a.Outcome = "rejected_http_status"
					}
				} else if outcome == "body_read_error" || outcome == "content_length_mismatch" {
					retry = true
				}
			}
			a.FinishedAt = stamp()
			result.Attempts = append(result.Attempts, a)
			if _, err := writeJSON(root, prefix+".json", a); err != nil {
				return result, err
			}
			if selected != nil {
				break
			}
			if ctx.Err() != nil {
				return stop("incomplete", "canceled_or_deadline")
			}
			if a.Outcome == "header_or_byte_budget" || a.Outcome == "body_or_byte_budget" {
				return stop("incomplete", a.Outcome)
			}
			if !retry {
				return stop("blocked", a.Outcome)
			}
			if delay > 30*time.Second {
				return stop("incomplete", "retry_deferred")
			}
			if attempt == r.Limits.AttemptsPerPage {
				return stop("incomplete", "attempt_budget")
			}
			if wait(ctx, delay) != nil {
				return stop("incomplete", "canceled_or_deadline")
			}
		}
		capture.Pages = append(capture.Pages, *selected)
		checkpoint := fmt.Sprintf("checkpoint-%03d.json", page)
		if _, err := writeJSON(root, checkpoint, capture); err != nil {
			return result, err
		}
		review, err := ReadCapture(ctx, filepath.Join(directory, checkpoint))
		if err != nil {
			if ctx.Err() != nil {
				return stop("incomplete", "canceled_or_deadline")
			}
			return stop("blocked", "capture_reader_rejected")
		}
		if _, err := writeJSON(root, fmt.Sprintf("review-%03d.json", page), review); err != nil {
			return result, err
		}
		if review.State != "validated_observations" {
			return stop("blocked", "source_review_issues")
		}
		// Do not infer termination from count/pages, even when exact.
		if review.PaginationState == "empty_page_observed" {
			capRef, err := writeJSON(root, "capture.json", capture)
			if err != nil {
				return result, err
			}
			// The final descriptor has the same bytes as the accepted checkpoint.
			if review.CaptureSHA256 != capRef.SHA256 {
				return result, errors.New("capture descriptor identity mismatch")
			}
			reviewRef, err := writeJSON(root, "review.json", review)
			if err != nil {
				return result, err
			}
			result.Capture = &capRef
			result.Review = &reviewRef
			return stop("captured", "query_empty_page_observed")
		}
	}
	return stop("incomplete", "page_budget")
}

func stamp() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func writeJSON(root *os.Root, path string, value any) (Artifact, error) {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return Artifact{}, errors.New("cannot encode metadata evidence")
	}
	return writeBytes(root, path, append(b, '\n'))
}

// Private, newly-created run directory; filenames are internal constants. Each
// write is exclusive and synced. A failed/partial artifact is never accepted.
func writeBytes(root *os.Root, path string, b []byte) (Artifact, error) {
	f, err := root.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Artifact{}, errors.New("cannot create exclusive metadata artifact")
	}
	_, writeErr := f.Write(b)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return Artifact{}, errors.New("cannot finish metadata artifact")
	}
	return Artifact{Path: path, Bytes: int64(len(b)), SHA256: digest(b)}, nil
}
