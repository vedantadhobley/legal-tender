package flowevidence

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// ReadQuery has no arbitrary AQL, database, collection, or filesystem input.
type ReadQuery struct {
	Kind      string `json:"kind"`
	Ledger    Ledger `json:"ledger"`
	Committee string `json:"committee,omitempty"`
	Direction string `json:"direction,omitempty"`
	Component string `json:"component,omitempty"`
	Target    string `json:"target,omitempty"`
	Depth     int    `json:"max_depth,omitempty"`
	Limit     int    `json:"limit"`
	After     string `json:"-"`
}

type ReadPage struct {
	Items   []json.RawMessage `json:"items"`
	HasMore bool              `json:"has_more"`
	Last    string            `json:"-"`
}

type ReadPath struct {
	Key          string            `json:"key"`
	Vertices     []json.RawMessage `json:"vertices"`
	Observations []json.RawMessage `json:"observations"`
}

func (q ReadQuery) Validate() error {
	if _, err := edgeCollection(q.Ledger); q.Kind != "entities" && err != nil {
		return ErrInvalidQuery
	}
	if q.Limit < 1 || q.Limit > 100 || len(q.After) > 2048 {
		return ErrInvalidQuery
	}
	switch q.Kind {
	case "entities":
		if q.Ledger != "" || q.Committee != "" || q.Direction != "" || q.Component != "" || q.Target != "" || q.Depth != 0 || q.After != "" && !committeeKey(q.After) {
			return ErrInvalidQuery
		}
	case "observations":
		if !committeeKey(q.Committee) || q.Direction != "inbound" && q.Direction != "outbound" && q.Direction != "any" || q.Component != "" || q.Target != "" || q.Depth != 0 || q.After != "" && !digestKey(q.After) {
			return ErrInvalidQuery
		}
	case "members":
		if !digestKey(q.Component) || q.Committee != "" || q.Direction != "" || q.Target != "" || q.Depth != 0 || q.After != "" && !digestKey(q.After) {
			return ErrInvalidQuery
		}
	case "paths", "neighborhood", "cycles", "shortest":
		if !committeeKey(q.Committee) || q.Direction != "" || q.Component != "" || q.Depth < 1 || q.Depth > 8 || q.Limit > 25 {
			return ErrInvalidQuery
		}
		if q.Kind == "paths" || q.Kind == "shortest" {
			if !committeeKey(q.Target) {
				return ErrInvalidQuery
			}
		} else if q.Target != "" {
			return ErrInvalidQuery
		}
		if q.Kind == "shortest" && (q.After != "" || q.Limit != 1) {
			return ErrInvalidQuery
		}
	default:
		return ErrInvalidQuery
	}
	return nil
}

func readQuery(q ReadQuery) (string, map[string]any, error) {
	if err := q.Validate(); err != nil {
		return "", nil, err
	}
	collection, _ := edgeCollection(q.Ledger)
	bind := map[string]any{"after": q.After, "limit": q.Limit + 1}
	if q.Kind == "entities" {
		return "FOR d IN entities FILTER d._key > @after SORT d._key LIMIT @limit RETURN UNSET(d, '_id', '_rev')", bind, nil
	}
	if q.Kind == "observations" || q.Kind == "members" {
		bind["@collection"] = collection
		filter := "d.component_id == @component"
		if q.Kind == "members" {
			bind["component"] = q.Component
		} else {
			bind["committee"] = entityID(q.Committee)
			switch q.Direction {
			case "inbound":
				filter = "d._to == @committee"
			case "outbound":
				filter = "d._from == @committee"
			case "any":
				filter = "(d._from == @committee OR d._to == @committee)"
			}
		}
		return "FOR d IN @@collection FILTER d._key > @after AND " + filter + " SORT d._key LIMIT @limit RETURN UNSET(d, '_id', '_rev')", bind, nil
	}
	bind["start"], bind["depth"] = entityID(q.Committee), q.Depth
	direction, unique, filter := "OUTBOUND", "path", ""
	switch q.Kind {
	case "neighborhood":
		direction, unique = "ANY", "none"
	case "cycles":
		unique, filter = "none", "FILTER v._id == @start"
	case "paths", "shortest":
		bind["target"] = entityID(q.Target)
		filter = "FILTER v._id == @target"
		if q.Target == q.Committee {
			unique = "none"
		}
	}
	if q.Kind == "shortest" {
		bind["limit"] = 1
	}
	// Stable total order, including ties between parallel observations. No
	// backend cursor escapes to HTTP. Sorting is bounded by query time/memory,
	// not by a misleading pre-sort sample; a resource failure returns no page.
	query := fmt.Sprintf("WITH entities FOR v,e,p IN 1..@depth %s @start %s OPTIONS {order:'bfs',uniqueVertices:'%s',uniqueEdges:'path'} %s LET key=CONCAT(TO_STRING(LENGTH(p.edges)), ':', CONCAT_SEPARATOR(':', p.edges[*]._id)) FILTER key > @after SORT key LIMIT @limit RETURN {key:key, vertices:(FOR n IN p.vertices RETURN UNSET(n, '_id', '_rev')), observations:(FOR o IN p.edges RETURN UNSET(o, '_id', '_rev'))}", direction, collection, unique, filter)
	return query, bind, nil
}

func (r *Reader) Query(ctx context.Context, q ReadQuery) (ReadPage, error) {
	query, bind, err := readQuery(q)
	if err != nil {
		return ReadPage{}, err
	}
	if q.Committee != "" {
		if _, ok := r.expectedEntity(q.Committee); !ok {
			return ReadPage{}, ErrNotFound
		}
	}
	if q.Target != "" {
		if _, ok := r.expectedEntity(q.Target); !ok {
			return ReadPage{}, ErrNotFound
		}
	}
	if q.Component != "" {
		if _, ok := r.expectedComponent(q.Component); !ok {
			return ReadPage{}, ErrNotFound
		}
	}
	page := ReadPage{Items: []json.RawMessage{}}
	last := q.After
	err = r.c.query(ctx, query, bind, 5, func(raw json.RawMessage) error {
		if len(page.Items) > q.Limit {
			return fmt.Errorf("backend exceeded page bound")
		}
		key := ""
		if q.Kind == "entities" {
			var e entity
			if err := json.Unmarshal(raw, &e); err != nil {
				return err
			}
			want, ok := r.expectedEntity(e.Key)
			if !ok || !equalDocument(raw, want) {
				return fmt.Errorf("entity differs from verified publication")
			}
			key = e.Key
		} else if q.Kind == "observations" || q.Kind == "members" {
			var e observation
			if err := json.Unmarshal(raw, &e); err != nil {
				return err
			}
			want, ok := r.expectedEdge(q.Ledger, e.Key)
			if !ok || !equalDocument(raw, want) {
				return fmt.Errorf("observation differs from verified publication")
			}
			if q.Kind == "members" && e.ComponentID != q.Component {
				return fmt.Errorf("foreign component member")
			}
			if q.Kind == "observations" && !(q.Direction != "inbound" && e.Sender == q.Committee || q.Direction != "outbound" && e.Recipient == q.Committee) {
				return fmt.Errorf("foreign neighborhood observation")
			}
			key = e.Key
		} else {
			var p ReadPath
			if err := json.Unmarshal(raw, &p); err != nil {
				return err
			}
			if err := r.validateReadPath(p, q); err != nil {
				return err
			}
			key = p.Key
		}
		if key <= last {
			return fmt.Errorf("non-increasing evidence page")
		}
		last = key
		page.Items = append(page.Items, raw)
		if len(page.Items) <= q.Limit {
			page.Last = key
		}
		return nil
	})
	if err != nil {
		return ReadPage{}, err
	}
	page.HasMore = len(page.Items) > q.Limit
	if page.HasMore {
		page.Items = page.Items[:q.Limit]
	}
	return page, nil
}

func (r *Reader) validateReadPath(p ReadPath, q ReadQuery) error {
	if len(p.Observations) < 1 || len(p.Observations) > q.Depth || len(p.Vertices) != len(p.Observations)+1 {
		return fmt.Errorf("invalid path shape")
	}
	path := pathRow{}
	index := map[string]observation{}
	seenVertices := map[string]bool{}
	for _, raw := range p.Vertices {
		var v entity
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		want, ok := r.expectedEntity(v.Key)
		if !ok || !equalDocument(raw, want) {
			return fmt.Errorf("path vertex differs from verified publication")
		}
		if (q.Kind == "paths" || q.Kind == "shortest") && q.Committee != q.Target && seenVertices[v.Key] {
			return fmt.Errorf("repeated vertex in a simple path")
		}
		seenVertices[v.Key] = true
		path.Vertices = append(path.Vertices, entityID(v.Key))
	}
	collection, _ := edgeCollection(q.Ledger)
	for _, raw := range p.Observations {
		var e observation
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		want, ok := r.expectedEdge(q.Ledger, e.Key)
		if !ok || !equalDocument(raw, want) {
			return fmt.Errorf("path observation differs from verified publication")
		}
		path.Edges = append(path.Edges, collection+"/"+e.Key)
		index[e.Key] = want
	}
	if p.Key != strconv.Itoa(len(path.Edges))+":"+strings.Join(path.Edges, ":") {
		return fmt.Errorf("path ordering key mismatch")
	}
	return validatePath(path, pathRequest{q.Ledger, q.Kind, entityID(q.Committee), entityID(q.Target), q.Depth, q.Limit}, index)
}
