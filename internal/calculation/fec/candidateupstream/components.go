package candidateupstream

import (
	"context"

	"github.com/vedantadhobley/legal-tender/internal/graphutil"
)

// Retain the candidate-scoped boundary while sharing the unchanged SCC engine.
func strongComponents(ctx context.Context, nodes []string, adj, reverse map[string][]string) ([][]string, error) {
	return graphutil.StrongComponents(ctx, nodes, adj, reverse)
}
