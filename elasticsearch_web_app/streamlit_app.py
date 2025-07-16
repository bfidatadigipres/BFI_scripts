import streamlit as st
from streamlit_dynamic_filters import DynamicFilters
import pandas as pd
from elasticsearch import Elasticsearch
import elasticsearch_helper as elastic_helper
import os

ELASTIC_PASS = os.environ['ELASTIC_PASS']
ELASTIC_PATH = os.environ['ELASTIC_PATH']
ELASIC_USERNAME = os.environ['ELASIC_USERNAME']

st.set_page_config(page_title='Filename Search Engine', page_icon='', layout='wide')
text_search = st.text_input("Search Filename", value='')

# getting the filename
es = Elasticsearch(ELASTIC_PATH,
    basic_auth=("elastic", elastic_helper.ELASTIC_PASS),
    verify_certs=False)

autoingest_index =[]

autoingest_index = elastic_helper.get_autoingest_docs(es)
df = elastic_helper.search_by_filepath(text_search, autoingest_index, es)


with st.sidebar:
    st.write('Apply filter: ')

    filter_column = ['log_level']
    dynamic_filter= DynamicFilters(df, filters=filter_column)

    dynamic_filter.display_filters(location='sidebar')
    filtered_df = dynamic_filter.filter_df()
if text_search and not df.empty:
    st.write('results: ')
    st.dataframe(filtered_df)
else:
    st.write('no results are found!!!')
