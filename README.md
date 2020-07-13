# Vaxonomics
#### <a href='https://github.com/VeeLeeKoh'>Vee Lee Koh</a> and <a href='https://github.com/noahplacke'>Noah Placke</a> 
---
### Data modeling and dataflow programming of vaccine and drug datasets
Data warehouse includes:
<ol>
  <li> Modeling - GCP, SQL, BigQuery and Lucidchart </li>
  <li> Transformation - Dataflow and Apache Beam </li>
  <li> Integration - GCP and SQL </li>
  <li> Processing pipeline - Airflow </li>
  <li> Visualization - Data Studio </li>
</ol>

The primary dataset is the **Vaccine Adverse Event Reporting System (VAERS) 2018 by the Centers for Disease Control and Prevention (CDC)**, which contains the list of reported adverse events after a vaccine was administered as well as details regarding the patient, vaccine and symptoms experienced.

The secondary dataset is the **FDA Adverse Event Reporting System (FAERS) 2018 Q4 by the Food and Drug Administration (FDA)**, which is a collection of reported adverse reactions for prescription drugs with details of demographics of patients, drugs, outcomes and diagnoses.

More information on the datasets can be found in <a href='DATASETS.txt'>DATASETS.txt</a>.

---
### References
- Primary dataset source: <a href='https://vaers.hhs.gov/data/datasets.html'> VAERS 2018 by CDC </a>
- Secondary dataset source: <a href='https://data.nber.org/data/fda-adverse-event-reporting-system-faers-data.html'> FAERS 2018 Q4 by FDA </a>
