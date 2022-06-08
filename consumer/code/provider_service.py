# This is the Avro schema defined by SpoolDir from CSV header.
SCHEMA_STR = """
{
  "connect.name": "com.github.jcustenborder.kafka.connect.model.Value",
  "fields": [
    {
      "default": null,
      "name": "Rndrng_NPI",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Last_Org_Name",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_First_Name",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_MI",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Crdntls",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Gndr",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Ent_Cd",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_St1",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_St2",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_City",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_State_Abrvtn",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_State_FIPS",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Zip5",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_RUCA",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_RUCA_Desc",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Cntry",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Type",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "HCPCS_Cd",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "HCPCS_Desc",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "HCPCS_Drug_Ind",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Place_Of_Srvc",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Tot_Benes",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Tot_Srvcs",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Tot_Bene_Day_Srvcs",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Avg_Sbmtd_Chrg",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Avg_Mdcr_Alowd_Amt",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Avg_Mdcr_Pymt_Amt",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "default": null,
      "name": "Avg_Mdcr_Stdzd_Amt",
      "type": [
        "null",
        "string"
      ]
    }
  ],
  "name": "Value",
  "namespace": "com.github.jcustenborder.kafka.connect.model",
  "type": "record"
}
"""

class ProviderService:
    def __init__(self, msg_payload_json, nomi):
        self.prvdr_id = int(float(msg_payload_json['Rndrng_NPI']))
        self.prvdr_name = msg_payload_json['Rndrng_Prvdr_Last_Org_Name']
        self.prvdr_ent = msg_payload_json['Rndrng_Prvdr_Ent_Cd'] 
        self.prvdr_st1  = msg_payload_json['Rndrng_Prvdr_St1']
        self.prvdr_st2 = msg_payload_json['Rndrng_Prvdr_St2']  # can be empty/null
        self.prvdr_city = msg_payload_json['Rndrng_Prvdr_City'] 
        self.prvdr_state = msg_payload_json['Rndrng_Prvdr_State_Abrvtn']
        self.area_desc = msg_payload_json['Rndrng_Prvdr_RUCA_Desc']
        
        # Geo localization using ZIP code
        geo_info = nomi.query_postal_code(msg_payload_json['Rndrng_Prvdr_Zip5'])
        self.latitude = geo_info.latitude
        self.longitude = geo_info.longitude
        
        self.prvdr_spec = msg_payload_json['Rndrng_Prvdr_Type']
        self.srvc_code = msg_payload_json['HCPCS_Cd']
        
        # Some descriptions have quote characters at the middle, which are removed here
        self.srvc_desc = msg_payload_json['HCPCS_Desc'].replace("'", "").replace('"', '')

        self.place_of_svc = msg_payload_json['Place_Of_Srvc']

        # Those columns are integers but some values have .0 at the end. Integers are required to store in PG.
        self.tot_users = int(float(msg_payload_json['Tot_Benes']))
        self.tot_srvcs = int(float(msg_payload_json['Tot_Srvcs']))
        
        self.avg_chrg_amt = round(float(msg_payload_json['Avg_Sbmtd_Chrg']), 2)
        
        # Compute average percentual amount paid by medicare out of charged amount
        self.avg_prcnt_mdcr_amt = round((float(msg_payload_json['Avg_Mdcr_Pymt_Amt']) / float(msg_payload_json['Avg_Sbmtd_Chrg'])) * 100.0, 2)

        
    def to_tuple(self):
        # Reducing number of columns to improve ingestion rate
        return (self.prvdr_id, self.latitude, self.longitude, self.prvdr_spec, self.srvc_desc, 
                self.tot_users, self.tot_srvcs, self.avg_chrg_amt, self.avg_prcnt_mdcr_amt,
                self.prvdr_state)