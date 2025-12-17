import streamlit as st
import duckdb
import polars as pl

st.set_page_config(page_title="F1 Dashboard", layout="wide")

st.title("Formula 1 – Data Engineering Dashboard")

DB_PATH = "/data/warehouse.duckdb"


con = duckdb.connect(DB_PATH, read_only=True)


tables = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    ORDER BY table_schema, table_name
""").pl()

st.subheader("Tabelle presenti nel database")
st.dataframe(tables)


st.subheader("Anteprima gare (bronze.races)")
races = con.execute("SELECT * FROM bronze.races LIMIT 20").pl()
st.dataframe(races)

con.close()
