package personaffiliation

import (
	"context"

	"github.com/vedantadhobley/legal-tender/internal/source/wikimedia"
)

// DiscoveryPlan projects only verified as-filed appearances from the diagnostic
// corpus. Reviewed roles, subject IDs and expected outcomes never select queries.
// The generic planner accepts other source adapters' verified appearances too.
func DiscoveryPlan(ctx context.Context, directory, pin, build string) (wikimedia.DiscoveryPlan, error) {
	return DiscoveryPlanWithPolicy(ctx, directory, pin, build, wikimedia.DiscoveryPolicy)
}

func DiscoveryPlanWithPolicy(ctx context.Context, directory, pin, build, policy string) (wikimedia.DiscoveryPlan, error) {
	corpus, err := Run(ctx, directory, pin)
	if err != nil {
		return wikimedia.DiscoveryPlan{}, err
	}
	inputs := make([]wikimedia.DiscoveryAppearance, 0, len(corpus.Cases))
	for _, c := range corpus.Cases {
		a := c.Assessment.Appearance
		inputs = append(inputs, wikimedia.DiscoveryAppearance{SHA256: a.Source.SHA256, Locator: a.Source.Locator, Name: a.Receipt.Name, Employer: a.Receipt.Employer})
	}
	return wikimedia.PlanDiscoveryWithPolicy(inputs, "verified as-filed appearances selected by diagnostic corpus "+pin, build, policy)
}
