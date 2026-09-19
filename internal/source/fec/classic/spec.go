// Package classic parses the FEC's small, pipe-delimited cycle bulk products.
package classic

import (
	"fmt"
	"regexp"
	"time"
)

const ParserVersion = "legal-tender.fec.classic-occurrence-parser.v2"

type Dataset string

const (
	CandidateMaster           Dataset = "candidate-master"
	CommitteeMaster           Dataset = "committee-master"
	CandidateCommitteeLinkage Dataset = "candidate-committee-linkage"
	AllCandidatesSummary      Dataset = "all-candidates-summary"
	CurrentCampaignsSummary   Dataset = "current-campaigns-summary"
)

type Spec struct {
	Dataset        Dataset
	Code           string
	SourceContract string
	FactType       string
	Fields         []string
	NaturalKey     int
}

var candidateFields = []string{
	"CAND_ID", "CAND_NAME", "CAND_PTY_AFFILIATION", "CAND_ELECTION_YR",
	"CAND_OFFICE_ST", "CAND_OFFICE", "CAND_OFFICE_DISTRICT", "CAND_ICI",
	"CAND_STATUS", "CAND_PCC", "CAND_ST1", "CAND_ST2", "CAND_CITY",
	"CAND_ST", "CAND_ZIP",
}

var committeeFields = []string{
	"CMTE_ID", "CMTE_NM", "TRES_NM", "CMTE_ST1", "CMTE_ST2", "CMTE_CITY",
	"CMTE_ST", "CMTE_ZIP", "CMTE_DSGN", "CMTE_TP", "CMTE_PTY_AFFILIATION",
	"CMTE_FILING_FREQ", "ORG_TP", "CONNECTED_ORG_NM", "CAND_ID",
}

var linkageFields = []string{
	"CAND_ID", "CAND_ELECTION_YR", "FEC_ELECTION_YR", "CMTE_ID", "CMTE_TP",
	"CMTE_DSGN", "LINKAGE_ID",
}

var summaryFields = []string{
	"CAND_ID", "CAND_NAME", "CAND_ICI", "PTY_CD", "CAND_PTY_AFFILIATION",
	"TTL_RECEIPTS", "TRANS_FROM_AUTH", "TTL_DISB", "TRANS_TO_AUTH", "COH_BOP",
	"COH_COP", "CAND_CONTRIB", "CAND_LOANS", "OTHER_LOANS", "CAND_LOAN_REPAY",
	"OTHER_LOAN_REPAY", "DEBTS_OWED_BY", "TTL_INDIV_CONTRIB", "CAND_OFFICE_ST",
	"CAND_OFFICE_DISTRICT", "SPEC_ELECTION", "PRIM_ELECTION", "RUN_ELECTION",
	"GEN_ELECTION", "GEN_ELECTION_PRECENT", "OTHER_POL_CMTE_CONTRIB",
	"POL_PTY_CONTRIB", "CVG_END_DT", "INDIV_REFUNDS", "CMTE_REFUNDS",
}

var specs = map[Dataset]Spec{
	CandidateMaster: {
		Dataset: CandidateMaster, Code: "cn", SourceContract: "fec/candidate-master@1.0.0",
		FactType: "fec.candidate_assertion.v1", Fields: candidateFields, NaturalKey: 0,
	},
	CommitteeMaster: {
		Dataset: CommitteeMaster, Code: "cm", SourceContract: "fec/committee-master@1.0.0",
		FactType: "fec.committee_assertion.v1", Fields: committeeFields, NaturalKey: 0,
	},
	CandidateCommitteeLinkage: {
		Dataset: CandidateCommitteeLinkage, Code: "ccl", SourceContract: "fec/candidate-committee-linkage@1.0.0",
		FactType: "fec.candidate_committee_linkage.v1", Fields: linkageFields, NaturalKey: 6,
	},
	AllCandidatesSummary: {
		Dataset: AllCandidatesSummary, Code: "weball", SourceContract: "fec/all-candidates-summary@1.0.0",
		FactType: "fec.candidate_summary_all.v1", Fields: summaryFields, NaturalKey: 0,
	},
	CurrentCampaignsSummary: {
		Dataset: CurrentCampaignsSummary, Code: "webl", SourceContract: "fec/current-campaigns-summary@1.0.0",
		FactType: "fec.campaign_summary.v1", Fields: summaryFields, NaturalKey: 0,
	},
}

func Lookup(name string) (Spec, error) {
	spec, ok := specs[Dataset(name)]
	if !ok {
		return Spec{}, fmt.Errorf("unsupported classic FEC dataset %q", name)
	}
	spec.Fields = append([]string(nil), spec.Fields...)
	return spec, nil
}

func Datasets() []Dataset {
	return []Dataset{
		CandidateMaster,
		CommitteeMaster,
		CandidateCommitteeLinkage,
		AllCandidatesSummary,
		CurrentCampaignsSummary,
	}
}

func (spec Spec) ValidNaturalKey(value string) bool {
	switch spec.Dataset {
	case CandidateMaster, AllCandidatesSummary, CurrentCampaignsSummary:
		return candidateIDPattern.MatchString(value)
	case CommitteeMaster:
		return committeeIDPattern.MatchString(value)
	case CandidateCommitteeLinkage:
		return digitsPattern.MatchString(value)
	default:
		return false
	}
}

var (
	candidateIDPattern = regexp.MustCompile(`^[HPS][A-Z0-9]{8}$`)
	committeeIDPattern = regexp.MustCompile(`^C[0-9]{8}$`)
	yearPattern        = regexp.MustCompile(`^[0-9]{4}$`)
	digitsPattern      = regexp.MustCompile(`^[0-9]+$`)
	moneyPattern       = regexp.MustCompile(`^$|^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)
)

func (spec Spec) validateFields(fields []string, cycle string) []ValidationIssue {
	issues := make([]ValidationIssue, 0, 2)
	addPattern := func(index int, pattern *regexp.Regexp, code string) {
		if !pattern.MatchString(fields[index]) {
			issues = append(issues, ValidationIssue{Code: code, Message: fmt.Sprintf("field %d (%s) has invalid source syntax", index+1, spec.Fields[index])})
		}
	}
	switch spec.Dataset {
	case CandidateMaster:
		addPattern(0, candidateIDPattern, "invalid_candidate_id")
		addPattern(3, yearPattern, "invalid_candidate_election_year")
	case CommitteeMaster:
		addPattern(0, committeeIDPattern, "invalid_committee_id")
		if fields[14] != "" && !candidateIDPattern.MatchString(fields[14]) {
			issues = append(issues, ValidationIssue{Code: "invalid_candidate_id", Message: "field 15 (CAND_ID) has invalid source syntax"})
		}
	case CandidateCommitteeLinkage:
		addPattern(0, candidateIDPattern, "invalid_candidate_id")
		addPattern(1, yearPattern, "invalid_candidate_election_year")
		addPattern(2, yearPattern, "invalid_fec_election_year")
		addPattern(3, committeeIDPattern, "invalid_committee_id")
		addPattern(6, digitsPattern, "invalid_linkage_id")
		if fields[2] != cycle {
			issues = append(issues, ValidationIssue{Code: "period_mismatch", Message: fmt.Sprintf("field 3 (FEC_ELECTION_YR) is %q; want source period %q", fields[2], cycle)})
		}
	case AllCandidatesSummary, CurrentCampaignsSummary:
		addPattern(0, candidateIDPattern, "invalid_candidate_id")
		for _, index := range []int{5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 25, 26, 28, 29} {
			if !moneyPattern.MatchString(fields[index]) {
				issues = append(issues, ValidationIssue{Code: "invalid_money", Message: fmt.Sprintf("field %d (%s) has invalid money syntax", index+1, spec.Fields[index])})
			}
		}
		if value := fields[27]; value != "" {
			parsed, err := time.Parse("01/02/2006", value)
			if err != nil || parsed.Format("01/02/2006") != value {
				issues = append(issues, ValidationIssue{Code: "invalid_coverage_date", Message: "field 28 (CVG_END_DT) is not a valid MM/DD/YYYY date"})
			}
		}
	}
	return issues
}
