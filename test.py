import streamlit as st
from databricks import sql

st.title("Databricks Test")

conn = sql.connect(
    server_hostname=st.secrets["SERVER_HOSTNAME"],
    http_path=st.secrets["HTTP_PATH"],
    access_token=st.secrets["ACCESS_TOKEN"]
)

cursor = conn.cursor()
cursor.execute("SELECT 1")
rows = cursor.fetchall()

st.write(rows)