# Instructions and flow description
- Have docker and docker-compose installed in the local machine.
- Download CSV dataset at https://data.cms.gov/provider-summary-by-type-of-service/medicare-physician-other-practitione
rs/medicare-physician-other-practitioners-by-provider-and-service and put it inside the folder `producer/data/unprocessed` (file name is not important).
- Run project using: `docker-compose up` from project root folder.
- After all images are built, project will start running by setting Kafka infrastructure. Ingestion starts at the 2 min mark from `producer` container by launching a script that triggers spooldir connector in `connect` container.
- Navigate to Confluent Kafka Control Center (http://localhost:9021/) to check ingested messages to the `science` topic. When the entire dataset is loaded into kafka, the CSV file is going to be moved to `producer/data/processed`.
- At the 2 min 30 s mark, consumer will start polling messages from `science` topic, transforming them and ingesting into `postgres` database `science_db`, table `science.med_provider_services`.
- To monitor messages being consumed, please navigate to consumer container and run `tail -f /var/log/consumer.log`. A summary of ingested messages will be logged every 1k messages with format: `Messages consumed: <> | Fully Processed: <> | Skipped: <>'`, which can be used to estimate ingestion rate end to end.
- At the 4 min mark, `dashboard` container will start reading data from `postgres` and `streamlit` app will be available at http://localhost:8501/
- Clean up everything by running: `docker-compose down`


# Exploratory Data Analysis
If desired, to run the jupyter-lab notebook used for EDA, just make sure to have jupyter-lab and pandas installed and run `jupyter-lab` from project root folder. This was used only at the earlier stages of development and some content is outdated with respect to final implementation. 

Some of the following summary stats/metadata below were used to drive decisions about what to do:
- Size (csv): 3.206.087.491 bytes (2.98GB)
- 10,140,228 rows (rows are unique by each tuple of (Rndrng_NPI, HCPCS_Cd, Place_Of_Srvc))
- 1,093,367 distinct providers:
    - Rndrng_Prvdr_Ent_Cd (individual or org): I 1,033,965 (94.56%) / O 59,402
    - Rndrng_Prvdr_Mdcr_Prtcptg_Ind (Medicare participation): Y 1,092,104 (99.88%) / N 1,263
    - Rndrng_Prvdr_Cntry (country): 1,093,294 providers are from US (99.99%)
- 6,138 distinct services/procedures (HCPCS_Cd)
- 5,568 distinct services/procedures descriptions (HPCS_Desc)
- 99 distinct service specialties/types (Rndrng_Prvdr_Type)
- Place_Of_Srvc (facility or not): F 3,887,551 (38.33%) / O 6,252,677
- Only column containing considerable number of null values is Rndrng_Prvdr_St2. Rndrng_Prvdr_Zip5 has 2 null values (not from US), but there are a few rows in which the country is US, but the ZIP code is from a foreign location.


# Tools used
- Kafka infra: Confluent Platform docker-compose.yml
- Producer: SpoolDir connector by modifying `connect` container image of Confluent Platform
- Consumer: python, confluent_kafka, pgeocode, sqlalchemy
- Database: PostgreSQL
- Dashboard: python, streamlit, plotly, pandas, sqlalchemy


# Decisions, limitations and improvements
- Ingestion rate is low (50-65 messages processed per second, end to end - it would take more than 40 hours to process entire dataset), mostly due to Postgres ingestion and approach implemented (ingest 1 row at a time). Not a lot of effort was put on performance improvements.
- Due to low ingestion rate, insights extracted can be misleading (e.g., is this value really zero or is it because there is no data ingested this data point yet?). Fortunately, the dataset is shuffled and ingestion order is not biased towards some column.
- The original idea was to do a map visual in streamlit and a lot of elements to do that are still present in the pipeline (extract latitude and longitude, keep only rows base on US to limit map range, etc). Ultimately I decided to remove it because I found the map navigation to be really slow and due to ingestion going on the map was a bit sparse, which made it difficult to see metrics in it (by elevation, color scale, etc). A heatmap visual was used instead.
- Provider specialty and US state were used as the heatmap groupings because they have a reasonable cardinality (< 100). Most of the other interesting, more specific dimensions have huge cardinality and would need to be combined/aggregated beforehand (e.g., top N providers based on some metric). The procedure description can still be used as a filter, but only when a single provider specialty is select, which significantly reduces the range and prevents streamlit from crashing.