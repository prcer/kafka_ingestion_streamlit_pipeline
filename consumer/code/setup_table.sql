CREATE SCHEMA IF NOT EXISTS science AUTHORIZATION science_user;

CREATE TABLE IF NOT EXISTS science.med_provider_services(
    prvdr_id INT,
    latitude NUMERIC, 
    longitude NUMERIC, 
    prvdr_spec VARCHAR, 
    srvc_desc VARCHAR,
    tot_users INT,
    tot_srvcs INT,
    avg_chrg_amt NUMERIC,
    avg_prcnt_mdcr_amt NUMERIC,
    prvdr_state VARCHAR
);