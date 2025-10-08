# Legal Tender

## Project Overview

Legal Tender is a project to analyze the influence of donors on US politicians by:

1. Collecting a list of all current US Congress members and the President.
2. Gathering donor data for each politician (amounts, names, organizations).
3. Profiling each donor to determine which policies they support or oppose.
4. Fetching upcoming bill/voting data.
5. Using AI to compare bills with donor policy stances, scoring each bill for each donor.
6. Aggregating scores to predict how politicians may vote and how much influence donors have.

## Data Requirements

1. **Current Politicians**
	- List of all current US Congress members and the President.
	- Source: [ProPublica Congress API](https://projects.propublica.org/api-docs/congress-api/), [GovTrack](https://www.govtrack.us/developers/api)

2. **Donor Data**
	- List of donors for each politician, including donation amounts.
	- Source: [OpenSecrets API](https://www.opensecrets.org/open-data/api), [FEC API](https://api.open.fec.gov/developers/)

3. **Upcoming Bills**
	- Data on upcoming bills, including text and summaries.
	- Source: [ProPublica Congress API](https://projects.propublica.org/api-docs/congress-api/), [GovTrack](https://www.govtrack.us/developers/api)

## Next Steps

1. Set up API clients to fetch:
	- Current politicians
	- Donor data
	- Upcoming bills
2. Store and process the data for further analysis.
3. Develop AI/NLP modules for donor policy profiling and bill analysis.
4. Build scoring and prediction logic.
5. Visualize results.