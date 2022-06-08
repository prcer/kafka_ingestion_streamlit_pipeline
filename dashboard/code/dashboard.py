import time
import logging as log
import pandas as pd
import streamlit as st
import pydeck as pdk
import os
import plotly.express as px
from postgres_wrapper import connect, read_data

AVG_CHARGED_AMT = 'Average charged amount (USD)'
AVG_MED_PAID_PERCENT = 'Average Medicare paid percentage'
AVG_BENE_SERVICE = 'Average beneficiaries per service'

log.basicConfig(filename = "/var/log/dashboard.log",
                filemode = "w",
                format='%(asctime)s - %(levelname)s: %(message)s',
                level = log.DEBUG)

st.set_page_config(
    page_title="Healthcare providers in US",
    layout="wide"
)

# Run only once.
@st.experimental_singleton
def init_connection():
    return connect(os.environ['POSTGRES_DB_URL'])

pg_engine = init_connection()

# Cache data
@st.experimental_memo
def reload_data():
    return read_data(pg_engine, os.environ['POSTGRES_SCHEMA'], os.environ['POSTGRES_TABLE'])

# TODO: dashboard will "crash" at startup if there is no data in postgres table
df = reload_data()

st.title('Healthcare Providers in US')

# Filter data by specialty
spec_list = ['SELECT ALL'] + list(df['prvdr_spec'].unique())
spec_filter = st.selectbox('Select provider medical specialty:', spec_list)
proc_filter = None
if spec_filter != 'SELECT ALL':
    df = df[df['prvdr_spec'] == spec_filter]
    # Further filter data by procedure when a specialty is selected
    proc_list = ['SELECT ALL'] + list(df['srvc_desc'].unique())
    proc_filter = st.selectbox('Select medical procedure description under specialty:', proc_list)
    if proc_filter != 'SELECT ALL':
        df = df[df['srvc_desc'] == proc_filter]



metrics = [AVG_CHARGED_AMT, AVG_MED_PAID_PERCENT, AVG_BENE_SERVICE]
sel_metric = st.selectbox('Select metric to display:', metrics)
st.markdown(f"## {sel_metric} by Provider Specialty and US State")

if proc_filter and proc_filter != 'SELECT ALL':
    st.markdown(f'Showing data only for procedure "{proc_filter}"')

if sel_metric == AVG_CHARGED_AMT:
    z_col = "avg_chrg_amt"
elif sel_metric == AVG_MED_PAID_PERCENT:
    z_col = "avg_prcnt_mdcr_amt"
elif sel_metric == AVG_BENE_SERVICE:
    z_col = "tot_users"

# Plot heatmap based on selected metric
fig = px.density_heatmap(df, x="prvdr_state", y="prvdr_spec", z=z_col, 
                         histfunc='avg', 
                         labels={"prvdr_state": "Provider State", 
                                 "prvdr_spec": "Provider Specialty", 
                                 z_col: sel_metric.replace('Average', '')},
                         width=1400, height=800)
st.write(fig)

# TODO Map visualization using latitude and longitude. 
# This idea was put aside because the maps looked sparse (while still ingesting) and slow to navigate, 
#   while not providing useful insights (mostly due to sparsity).
