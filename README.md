# DRIVERs: Data systems Research to Identify driVers of Ethnic Racial Inequities in Maternal Mortality"

Funding agency: NIH/NIDDK <br/>
Award numer: 1R21MD019175 <br/>
Funding period: 11/2023 - 10/2025 <br/>
PI: Albert Hsu (MU) <br/>
Co-I: Xing Song (MU); Michelle Debbink (UTAH) <br/>
NIH RePORT site: https://reporter.nih.gov/project-details/10810469 <br/>
DROC request: # <br/>

### Study Overview

The objective of this funded project is to better understand factors associated with maternal mortality and severe maternal morbidity (SMM) disparities in the United States. The critical knowledge gap is a comprehensive understanding of causes of maternal morbidity and mortality in different populations, with an emphasis on preventable factors that may vary in different racial ethnic groups.  Our central hypothesis is that the root causes of maternal mortality have been changing over time; we propose to test this hypothesis in these two specific aims: 

Aim 1:  to use de-identified healthcare records from patient care encounters in the Oracle/Cerner Real-World EHR database to determine drivers associated with racial and socioeconomic disparities in maternal mortality and severe maternal morbidity in the 50 healthcare facilities in that database.

Aim 2:  to use de-identified linked data from the Greater Plains CollaborativeTM, PCORnet, social security death files, obituary files, and geocoding (to help identify relevant social determinants of health) to determine drivers associated with racial disparities in maternal mortality and severe maternal morbidity at both the University of Missouri, and at the University of Utah.

### GPC Site Scope of Work

Participating sites (MU and Utah) will receive two to three distributed queries from study coordinating site (MU), run the query locally against their PCORnet CDM DataMart or source EMR tables, and return patient-level, de-identified data to MU via secure transfer methods. Sites will be required to return result of data collection and curation queries according to following milestone schedule:

|Milestone|Activity|Timeframe|
|---------|--------|---------|
|1. Administrative preparation|MU and sites work together to complete all the administrative preparation work such as subcontracts, DUA, and IRB reliance|Year 1|
|2. Additional preparations|Both MU and Sites need to supplement CDM with additional data from local EMR system (e.g., populating DEATH_CAUSE table, Geocoding) if not currently available.|Year 1|
|3. Queries for data collection|Site receives two to three queries from MU for data collection, curation and quality checks (QC). Final data sets that passed the QC will be transferred to MU via secure transfer protocol. MU will centrally aggregate data and map geocoded ID to various American Community Survey variables as well as relevant social determinants indices.|Year 1 - 2|
|4. Results Dissemination|Queries, Models and Results will be disseminated to participating sites. Sites’ collaborators will contribute to results interpretation and development of manuscripts or other publications and presentations.|Year 2|

### Cohort Selection

- Delivery date between 01 January 2015 and 31 December 2023 (this is using the DEID date shifted data), which is identified by 
    - DRG: '765','766','767','768','774','775','783','784','785','786', '787','788','796','797','798','805','806','807'
    - CPT4: '59409', '59514', '59612', '59620'
    - ICD10-PCS: '10D00Z0', '10D00Z1','10D00Z2','10D07Z3', '10D07Z4', '10D07Z5','10D07Z6', '10D07Z7', '10D07Z8','10E0XZZ'
- At least 211 days since the last delivery event in the time period
- Maternal age between 10 years and 54 years of age

### Non-Human-Subject Determination

This study has been determined as Non-human-subject research, as only de-identified data will be exchanged. The NHS determination application and approval letter was obtained on 09/28/2023, which can be downloaded from below: 
- [NHS Determination Letter](./doc/DRIVERS-NHS-Determination-Letter.pdf)

