package personaffiliation

import (
	"context"

	"github.com/vedantadhobley/legal-tender/internal/source/gleif"
)

// EmployerRegistryPlan uses verified filing values only. Reviewed company URLs,
// subjects, role annotations and Wikimedia candidates never select requests.
func EmployerRegistryPlan(ctx context.Context, directory, pin, build string) (gleif.NamePlan, error) {
	r, err := Run(ctx, directory, pin)
	if err != nil {
		return gleif.NamePlan{}, err
	}
	inputs := make([]gleif.NameInput, 0, len(r.Cases))
	for _, c := range r.Cases {
		a := c.Assessment.Appearance
		inputs = append(inputs, gleif.NameInput{SHA256: a.Source.SHA256, Locator: a.Source.Locator, Name: a.Receipt.Employer})
	}
	return gleif.PlanNames(inputs, "verified employer fields selected by diagnostic corpus "+pin, build)
}
