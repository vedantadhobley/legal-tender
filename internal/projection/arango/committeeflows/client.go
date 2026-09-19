package committeeflows

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"strings"
	"time"
)

type arangoClient struct {
	base       *url.URL
	username   string
	password   string
	httpClient *http.Client
}

type arangoAPIError struct {
	StatusCode int
	ErrorNum   int    `json:"errorNum"`
	Message    string `json:"errorMessage"`
}

func (err *arangoAPIError) Error() string {
	return fmt.Sprintf("ArangoDB HTTP %d error %d: %s", err.StatusCode, err.ErrorNum, err.Message)
}

type collectionResponse struct {
	Name string `json:"name"`
	Type int    `json:"type"`
}

type importResponse struct {
	Created int `json:"created"`
	Errors  int `json:"errors"`
	Empty   int `json:"empty"`
	Updated int `json:"updated"`
	Ignored int `json:"ignored"`
}

type cursorResponse struct {
	Result  []json.RawMessage `json:"result"`
	HasMore bool              `json:"hasMore"`
	ID      string            `json:"id"`
}

type collectionFiguresResponse struct {
	Figures struct {
		Indexes struct {
			Count uint64 `json:"count"`
			Size  uint64 `json:"size"`
		} `json:"indexes"`
		DocumentsSize uint64 `json:"documentsSize"`
		CacheSize     uint64 `json:"cacheSize"`
	} `json:"figures"`
}

func newArangoClient(endpoint, username, password string) (*arangoClient, error) {
	if endpoint == "" || username == "" || password == "" {
		return nil, fmt.Errorf("ArangoDB endpoint, username, and password are required")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse ArangoDB endpoint: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("ArangoDB endpoint must use http or https")
	}
	if parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("ArangoDB endpoint must contain only scheme, host, and optional base path")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	return &arangoClient{
		base: parsed, username: username, password: password,
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}, nil
}

func (client *arangoClient) ensureDatabase(ctx context.Context, database string) error {
	if !strings.HasPrefix(database, "lt_flow_probe_") {
		return fmt.Errorf("refusing non-flow-probe database %q", database)
	}
	var version map[string]any
	err := client.doJSON(ctx, database, http.MethodGet, "/_api/version", nil, nil, &version)
	if err == nil {
		return nil
	}
	var apiErr *arangoAPIError
	if !asAPIError(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return err
	}
	request := struct {
		Name string `json:"name"`
	}{Name: database}
	if err := client.doJSON(ctx, "_system", http.MethodPost, "/_api/database", nil, request, nil); err != nil {
		if asAPIError(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
			return nil
		}
		return fmt.Errorf("create flow probe database: %w", err)
	}
	return nil
}

func (client *arangoClient) ensureCollection(ctx context.Context, database, name string, collectionType int) error {
	var existing collectionResponse
	err := client.doJSON(ctx, database, http.MethodGet, "/_api/collection/"+url.PathEscape(name), nil, nil, &existing)
	if err == nil {
		if existing.Name != name || existing.Type != collectionType {
			return fmt.Errorf("collection %s has type %d, expected %d", name, existing.Type, collectionType)
		}
		return nil
	}
	var apiErr *arangoAPIError
	if !asAPIError(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return err
	}
	request := struct {
		Name string `json:"name"`
		Type int    `json:"type"`
	}{Name: name, Type: collectionType}
	if err := client.doJSON(ctx, database, http.MethodPost, "/_api/collection", nil, request, &existing); err != nil {
		return fmt.Errorf("create collection %s: %w", name, err)
	}
	return nil
}

func (client *arangoClient) ensurePersistentIndex(ctx context.Context, database, collection, name string, fields []string) error {
	query := url.Values{"collection": []string{collection}}
	request := struct {
		Type   string   `json:"type"`
		Name   string   `json:"name"`
		Fields []string `json:"fields"`
		Sparse bool     `json:"sparse"`
		Unique bool     `json:"unique"`
	}{Type: "persistent", Name: name, Fields: fields}
	if err := client.doJSON(ctx, database, http.MethodPost, "/_api/index", query, request, nil); err != nil {
		return fmt.Errorf("ensure index %s: %w", name, err)
	}
	return nil
}

func (client *arangoClient) ensureGraph(ctx context.Context, database string) error {
	request := struct {
		Name            string           `json:"name"`
		EdgeDefinitions []edgeDefinition `json:"edgeDefinitions"`
		Orphans         []string         `json:"orphanCollections"`
	}{
		Name: GraphName,
		EdgeDefinitions: []edgeDefinition{{
			Collection: edgesCollection, From: []string{entitiesCollection}, To: []string{entitiesCollection},
		}},
		Orphans: []string{},
	}
	err := client.doJSON(ctx, database, http.MethodPost, "/_api/gharial", nil, request, nil)
	if err == nil {
		return nil
	}
	var apiErr *arangoAPIError
	if !asAPIError(err, &apiErr) || apiErr.StatusCode != http.StatusConflict {
		return fmt.Errorf("ensure named graph: %w", err)
	}
	var existing struct {
		Graph struct {
			Name            string           `json:"name"`
			EdgeDefinitions []edgeDefinition `json:"edgeDefinitions"`
		} `json:"graph"`
	}
	if err := client.doJSON(ctx, database, http.MethodGet, "/_api/gharial/"+GraphName, nil, nil, &existing); err != nil {
		return fmt.Errorf("read existing named graph: %w", err)
	}
	if existing.Graph.Name != GraphName || !sameEdgeDefinitions(existing.Graph.EdgeDefinitions, request.EdgeDefinitions) {
		return fmt.Errorf("existing named graph %s has incompatible edge definitions", GraphName)
	}
	return nil
}

type edgeDefinition struct {
	Collection string   `json:"collection"`
	From       []string `json:"from"`
	To         []string `json:"to"`
}

func sameEdgeDefinitions(left, right []edgeDefinition) bool {
	if len(left) != len(right) {
		return false
	}
	byCollection := make(map[string]edgeDefinition, len(left))
	for _, definition := range left {
		byCollection[definition.Collection] = definition
	}
	for _, definition := range right {
		existing, ok := byCollection[definition.Collection]
		if !ok || !reflect.DeepEqual(existing.From, definition.From) || !reflect.DeepEqual(existing.To, definition.To) {
			return false
		}
	}
	return true
}

func (client *arangoClient) importDocuments(ctx context.Context, database, collection string, documents any) (importResponse, error) {
	content, count, err := encodeJSONLines(documents)
	if err != nil {
		return importResponse{}, err
	}
	if count == 0 {
		return importResponse{}, nil
	}
	query := url.Values{
		"collection": []string{collection}, "type": []string{"documents"},
		"onDuplicate": []string{"replace"}, "complete": []string{"true"},
	}
	var response importResponse
	if err := client.doRaw(ctx, database, http.MethodPost, "/_api/import", query, "application/x-ndjson", content, &response); err != nil {
		return response, fmt.Errorf("import %s: %w", collection, err)
	}
	if response.Errors != 0 || response.Empty != 0 || response.Created+response.Updated+response.Ignored != count {
		return response, fmt.Errorf("import %s did not conserve documents: submitted=%d created=%d updated=%d ignored=%d errors=%d empty=%d", collection, count, response.Created, response.Updated, response.Ignored, response.Errors, response.Empty)
	}
	return response, nil
}

func (client *arangoClient) metadata(ctx context.Context, database, projectionID string) (projectionMetadata, bool, error) {
	var metadata projectionMetadata
	err := client.doJSON(ctx, database, http.MethodGet, "/_api/document/"+metadataCollection+"/"+url.PathEscape(projectionID), nil, nil, &metadata)
	if err == nil {
		return metadata, true, nil
	}
	var apiErr *arangoAPIError
	if asAPIError(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		return projectionMetadata{}, false, nil
	}
	return projectionMetadata{}, false, err
}

func (client *arangoClient) metadataV2(ctx context.Context, database, projectionID string) (projectionMetadataV2, bool, error) {
	var metadata projectionMetadataV2
	err := client.doJSON(ctx, database, http.MethodGet, "/_api/document/"+metadataCollection+"/"+url.PathEscape(projectionID), nil, nil, &metadata)
	if err == nil {
		return metadata, true, nil
	}
	var apiErr *arangoAPIError
	if asAPIError(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		return projectionMetadataV2{}, false, nil
	}
	return projectionMetadataV2{}, false, err
}

func (client *arangoClient) query(ctx context.Context, database, query string, bindVars map[string]any) ([]json.RawMessage, error) {
	request := struct {
		Query     string         `json:"query"`
		BindVars  map[string]any `json:"bindVars"`
		BatchSize int            `json:"batchSize"`
	}{Query: query, BindVars: bindVars, BatchSize: 10_000}
	var response cursorResponse
	if err := client.doJSON(ctx, database, http.MethodPost, "/_api/cursor", nil, request, &response); err != nil {
		return nil, err
	}
	result := append([]json.RawMessage(nil), response.Result...)
	for response.HasMore {
		if response.ID == "" {
			return nil, fmt.Errorf("ArangoDB cursor has more rows without an ID")
		}
		var next cursorResponse
		if err := client.doJSON(ctx, database, http.MethodPut, "/_api/cursor/"+url.PathEscape(response.ID), nil, nil, &next); err != nil {
			return nil, err
		}
		result = append(result, next.Result...)
		response = next
	}
	return result, nil
}

func (client *arangoClient) collectionFigures(ctx context.Context, database, collection string) (collectionFiguresResponse, error) {
	var response collectionFiguresResponse
	if err := client.doJSON(ctx, database, http.MethodGet, "/_api/collection/"+url.PathEscape(collection)+"/figures", nil, nil, &response); err != nil {
		return collectionFiguresResponse{}, err
	}
	return response, nil
}

func (client *arangoClient) doJSON(ctx context.Context, database, method, apiPath string, query url.Values, requestBody, responseBody any) error {
	var body io.Reader
	if requestBody != nil {
		content, err := json.Marshal(requestBody)
		if err != nil {
			return err
		}
		body = bytes.NewReader(content)
	}
	return client.do(ctx, database, method, apiPath, query, "application/json", body, responseBody)
}

func (client *arangoClient) doRaw(ctx context.Context, database, method, apiPath string, query url.Values, contentType string, content []byte, responseBody any) error {
	return client.do(ctx, database, method, apiPath, query, contentType, bytes.NewReader(content), responseBody)
}

func (client *arangoClient) do(ctx context.Context, database, method, apiPath string, query url.Values, contentType string, body io.Reader, responseBody any) error {
	requestURL := *client.base
	requestURL.Path = path.Join(client.base.Path, "_db", database, apiPath)
	requestURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), body)
	if err != nil {
		return err
	}
	request.SetBasicAuth(client.username, client.password)
	if body != nil {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		content, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		apiErr := &arangoAPIError{StatusCode: response.StatusCode, Message: strings.TrimSpace(string(content))}
		_ = json.Unmarshal(content, apiErr)
		return apiErr
	}
	if responseBody == nil {
		_, err := io.Copy(io.Discard, response.Body)
		return err
	}
	decoder := json.NewDecoder(response.Body)
	return decoder.Decode(responseBody)
}

func asAPIError(err error, target **arangoAPIError) bool {
	value, ok := err.(*arangoAPIError)
	if ok {
		*target = value
	}
	return ok
}

func encodeJSONLines(documents any) ([]byte, int, error) {
	value := reflect.ValueOf(documents)
	if value.Kind() != reflect.Slice {
		return nil, 0, fmt.Errorf("ArangoDB import documents must be a slice")
	}
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	for index := 0; index < value.Len(); index++ {
		if err := encoder.Encode(value.Index(index).Interface()); err != nil {
			return nil, 0, err
		}
	}
	return buffer.Bytes(), value.Len(), nil
}
