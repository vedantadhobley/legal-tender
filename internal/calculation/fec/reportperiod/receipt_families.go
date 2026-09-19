package reportperiod

import (
	"context"

	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportmetadata"
	"github.com/vedantadhobley/legal-tender/internal/source/fec/reportscope"
)

const ReceiptFamilyBindingVersion = "legal-tender.fec.receipt-family-binding.v1"
const ReceiptFamilyReportsVersion = "legal-tender.fec.receipt-family-reports.v1"
const ReceiptFamilyBindingVersionV2 = "legal-tender.fec.receipt-family-binding.v2"
const ReceiptFamilyReportsVersionV2 = "legal-tender.fec.receipt-family-reports.v2"

type ReceiptFamilyReports struct {
	Version     string                  `json:"version"`
	Membership  Review                  `json:"membership"`
	DocumentSet reportmetadata.Artifact `json:"document_set"`
	Bindings    []WindowBinding         `json:"bindings"`
}

func bindReceiptFamilyFields(ctx context.Context, m Review, document reportscope.Request) (FieldBindingReview, error) {
	return bindElectronicFields(ctx, m, document, reportscope.AssessReceiptFamilies, ReceiptFamilyBindingVersion)
}

func bindReceiptFamilyFieldsV2(ctx context.Context, m Review, document reportscope.Request) (FieldBindingReview, error) {
	return bindElectronicFields(ctx, m, document, reportscope.AssessReceiptFamiliesV2, ReceiptFamilyBindingVersionV2)
}

// ReviewReceiptFamilyReports binds each report independently. It does not sum
// windows, manufacture absent detail, or broaden existing seven-field outputs.
func ReviewReceiptFamilyReports(ctx context.Context, request WindowRequest) (ReceiptFamilyReports, error) {
	return reviewReceiptFamilyReports(ctx, request, ReceiptFamilyReportsVersion, bindReceiptFamilyFields)
}

func ReviewReceiptFamilyReportsV2(ctx context.Context, request WindowRequest) (ReceiptFamilyReports, error) {
	return reviewReceiptFamilyReports(ctx, request, ReceiptFamilyReportsVersionV2, bindReceiptFamilyFieldsV2)
}

func reviewReceiptFamilyReports(ctx context.Context, request WindowRequest, version string, bind func(context.Context, Review, reportscope.Request) (FieldBindingReview, error)) (ReceiptFamilyReports, error) {
	r, err := readWindowBindings(ctx, request, bind)
	if err != nil {
		return ReceiptFamilyReports{}, err
	}
	return ReceiptFamilyReports{version, r.Membership, r.DocumentSet, r.Bindings}, nil
}
