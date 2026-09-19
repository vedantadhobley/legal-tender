package fecscheduleab

import (
	"fmt"
	"math"
	"sort"
	"strconv"
)

const (
	stateExact      = "exact"
	stateCompatible = "compatible_date_disagreement"
	stateConflict   = "conflicting_amount"
	stateAmbiguous  = "ambiguous"
	stateAOnly      = "schedule_a_only"

	bDispositionExact      = "exact_candidate"
	bDispositionCompatible = "compatible_date_candidate"
	bDispositionConflict   = "conflicting_amount_candidate"
	bDispositionUnmatched  = "unmatched_known_endpoint"
	bDispositionOutside    = "outside_schedule_a_endpoints"
	bDispositionIneligible = "ineligible_physical_shape"
)

type endpointKey struct {
	source    uint32
	recipient uint32
}

type flowKey struct {
	endpointKey
	dateDays int32
	amount   int64
}

type amountKey struct {
	endpointKey
	amount int64
}

type dateKey struct {
	endpointKey
	dateDays int32
}

type flowObservation struct {
	SourceID    uint32
	RecipientID uint32
	DateDays    int32
	Amount      int64
}

func (observation flowObservation) exactKey() flowKey {
	return flowKey{
		endpointKey: endpointKey{source: observation.SourceID, recipient: observation.RecipientID},
		dateDays:    observation.DateDays,
		amount:      observation.Amount,
	}
}

type aGroup struct {
	rows uint64
}

type relaxedA struct {
	key  flowKey
	rows uint64
}

type bMatch struct {
	rows   uint64
	amount int64
}

type matcher struct {
	exactA       map[flowKey]aGroup
	amountA      map[amountKey]relaxedA
	dateA        map[dateKey]relaxedA
	endpoints    map[endpointKey]struct{}
	exactB       map[flowKey]bMatch
	compatibleB  map[flowKey]bMatch
	conflictB    map[flowKey]bMatch
	bDisposition map[string]uint64
}

func newMatcher(observations []flowObservation) (*matcher, error) {
	result := &matcher{
		exactA: make(map[flowKey]aGroup), amountA: make(map[amountKey]relaxedA),
		dateA: make(map[dateKey]relaxedA), endpoints: make(map[endpointKey]struct{}),
		exactB: make(map[flowKey]bMatch), compatibleB: make(map[flowKey]bMatch),
		conflictB: make(map[flowKey]bMatch), bDisposition: make(map[string]uint64),
	}
	for _, observation := range observations {
		key := observation.exactKey()
		group := result.exactA[key]
		group.rows++
		result.exactA[key] = group
		result.endpoints[key.endpointKey] = struct{}{}
		amount := amountKey{endpointKey: key.endpointKey, amount: key.amount}
		amountGroup := result.amountA[amount]
		amountGroup.rows++
		if amountGroup.rows == 1 {
			amountGroup.key = key
		}
		result.amountA[amount] = amountGroup
		date := dateKey{endpointKey: key.endpointKey, dateDays: key.dateDays}
		dateGroup := result.dateA[date]
		dateGroup.rows++
		if dateGroup.rows == 1 {
			dateGroup.key = key
		}
		result.dateA[date] = dateGroup
	}
	return result, nil
}

func (matcher *matcher) observeIneligibleB() {
	matcher.bDisposition[bDispositionIneligible]++
}

func (matcher *matcher) observeB(observation flowObservation) error {
	key := observation.exactKey()
	if _, exists := matcher.endpoints[key.endpointKey]; !exists {
		matcher.bDisposition[bDispositionOutside]++
		return nil
	}
	if _, exists := matcher.exactA[key]; exists {
		if err := addBMatch(matcher.exactB, key, observation.Amount); err != nil {
			return err
		}
		matcher.bDisposition[bDispositionExact]++
		return nil
	}
	amount := amountKey{endpointKey: key.endpointKey, amount: key.amount}
	if candidate, exists := matcher.amountA[amount]; exists && candidate.rows == 1 {
		if err := addBMatch(matcher.compatibleB, candidate.key, observation.Amount); err != nil {
			return err
		}
		matcher.bDisposition[bDispositionCompatible]++
		return nil
	}
	date := dateKey{endpointKey: key.endpointKey, dateDays: key.dateDays}
	if candidate, exists := matcher.dateA[date]; exists && candidate.rows == 1 {
		if err := addBMatch(matcher.conflictB, candidate.key, observation.Amount); err != nil {
			return err
		}
		matcher.bDisposition[bDispositionConflict]++
		return nil
	}
	matcher.bDisposition[bDispositionUnmatched]++
	return nil
}

func addBMatch(values map[flowKey]bMatch, key flowKey, amount int64) error {
	value := values[key]
	if (amount > 0 && value.amount > math.MaxInt64-amount) || (amount < 0 && value.amount < math.MinInt64-amount) {
		return fmt.Errorf("Schedule B candidate amount exceeds int64")
	}
	value.rows++
	value.amount += amount
	values[key] = value
	return nil
}

type stateAccumulator struct {
	groups, aRows, bRows uint64
	aAmount, bAmount     int64
}

func (matcher *matcher) candidateStates() ([]CandidateState, error) {
	states := map[string]stateAccumulator{
		stateExact: {}, stateCompatible: {}, stateConflict: {}, stateAmbiguous: {}, stateAOnly: {},
	}
	for key, a := range matcher.exactA {
		state := stateAOnly
		exact := matcher.exactB[key]
		compatible := matcher.compatibleB[key]
		conflict := matcher.conflictB[key]
		b := bMatch{rows: exact.rows + compatible.rows + conflict.rows}
		if err := addInt64(&b.amount, exact.amount); err != nil {
			return nil, err
		}
		if err := addInt64(&b.amount, compatible.amount); err != nil {
			return nil, err
		}
		if err := addInt64(&b.amount, conflict.amount); err != nil {
			return nil, err
		}
		if b.rows > 0 {
			state = stateAmbiguous
			if a.rows == 1 && b.rows == 1 {
				switch {
				case exact.rows == 1:
					state = stateExact
				case compatible.rows == 1:
					state = stateCompatible
				case conflict.rows == 1:
					state = stateConflict
				}
			}
		}
		value := states[state]
		value.groups++
		value.aRows += a.rows
		value.bRows += b.rows
		if err := addRepeatedAmount(&value.aAmount, key.amount, a.rows); err != nil {
			return nil, err
		}
		if err := addInt64(&value.bAmount, b.amount); err != nil {
			return nil, err
		}
		states[state] = value
	}
	order := []string{stateExact, stateCompatible, stateConflict, stateAmbiguous, stateAOnly}
	result := make([]CandidateState, 0, len(order))
	for _, state := range order {
		value := states[state]
		result = append(result, CandidateState{
			State: state, SignatureGroups: value.groups, ScheduleARows: value.aRows, ScheduleBRows: value.bRows,
			ScheduleAAmountMinorUnits: strconv.FormatInt(value.aAmount, 10), ScheduleBAmountMinorUnits: strconv.FormatInt(value.bAmount, 10),
		})
	}
	return result, nil
}

func addRepeatedAmount(total *int64, amount int64, count uint64) error {
	if count > math.MaxInt64 {
		return fmt.Errorf("row count exceeds signed range")
	}
	if amount != 0 && int64(count) > math.MaxInt64/absoluteInt64(amount) {
		return fmt.Errorf("Schedule A candidate amount exceeds int64")
	}
	return addInt64(total, amount*int64(count))
}

func absoluteInt64(value int64) int64 {
	if value == math.MinInt64 {
		return math.MaxInt64
	}
	if value < 0 {
		return -value
	}
	return value
}

func addInt64(total *int64, value int64) error {
	if (value > 0 && *total > math.MaxInt64-value) || (value < 0 && *total < math.MinInt64-value) {
		return fmt.Errorf("candidate amount sum exceeds int64")
	}
	*total += value
	return nil
}

func (matcher *matcher) dispositions() []NamedCount {
	order := []string{
		bDispositionExact, bDispositionCompatible, bDispositionConflict,
		bDispositionUnmatched, bDispositionOutside, bDispositionIneligible,
	}
	result := make([]NamedCount, 0, len(order))
	for _, name := range order {
		result = append(result, NamedCount{Name: name, Rows: matcher.bDisposition[name]})
	}
	return result
}

func sumStateRows(states []CandidateState) (aRows, bRows uint64) {
	for _, state := range states {
		aRows += state.ScheduleARows
		bRows += state.ScheduleBRows
	}
	return aRows, bRows
}

func sumDispositionRows(values []NamedCount) uint64 {
	var result uint64
	for _, value := range values {
		result += value.Rows
	}
	return result
}

func sortedNamedCounts(values map[string]uint64) []NamedCount {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]NamedCount, 0, len(names))
	for _, name := range names {
		result = append(result, NamedCount{Name: name, Rows: values[name]})
	}
	return result
}
