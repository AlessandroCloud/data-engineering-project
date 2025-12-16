import streamlit as st
import duckdb
import pandas as pd

st.set_page_config(page_title="F1 Dashboard", layout="wide")

st.title("Formula 1 – Data Engineering Dashboard")
st.write("Verifica connessione DuckDB e tabelle Bronze")

# Connessione al DB 
DB_PATH = "/data/warehouse.duckdb"

try:
    con = duckdb.connect(DB_PATH, read_only=True)
    st.success(f"Connesso a DuckDB: {DB_PATH}")
except Exception as e:
    st.error("Errore di connessione a DuckDB")
    st.exception(e)
    st.stop()

# Elenco tabelle
tables = con.execute("""
    SELECT table_schema, table_name
    FROM information_schema.tables
    ORDER BY table_schema, table_name
""").fetchdf()

st.subheader("Tabelle presenti nel database")
st.dataframe(tables, use_container_width=True)


st.subheader("Anteprima gare (bronze.races)")
df_races = con.execute("SELECT * FROM bronze.races LIMIT 20").fetchdf()
st.dataframe(df_races, use_container_width=True)

con.close()
