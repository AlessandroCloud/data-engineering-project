import os
import re

import polars as pl
import streamlit as st
from etl.utils import get_connection
import google.generativeai as genai


# --------------------------------------------------
# CONFIG GEMINI
# --------------------------------------------------

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
else:
    genai = None  # la useremo per capire se mostrare l'area Text-to-SQL


# --------------------------------------------------
# FUNZIONI DI ACCESSO AL GOLD (KPI, DATAFRAME)
# --------------------------------------------------

def get_years() -> list[int]:
    """Ritorna la lista delle stagioni disponibili nel GOLD."""
    con = get_connection()
    years = con.execute(
        "SELECT DISTINCT season_year FROM gold.dim_race ORDER BY season_year;"
    ).fetchall()
    con.close()
    return [y[0] for y in years]


def get_kpis(selected_years: list[int] | None = None) -> dict:
    """
    Restituisce alcuni KPI base (stagioni, gare, piloti, costruttori),
    eventualmente filtrati per un sottoinsieme di anni.
    """
    con = get_connection()

    if selected_years:
        years_tuple = tuple(selected_years)
        year_filter_dim = f"WHERE season_year IN {years_tuple}"
        year_filter_fact = f"WHERE season_year IN {years_tuple}"
    else:
        year_filter_dim = ""
        year_filter_fact = ""

    total_seasons = con.execute(
        f"SELECT COUNT(DISTINCT season_year) FROM gold.dim_race {year_filter_dim};"
    ).fetchone()[0]

    total_races = con.execute(
        f"SELECT COUNT(DISTINCT race_id) FROM gold.dim_race {year_filter_dim};"
    ).fetchone()[0]

    total_drivers = con.execute(
        f"SELECT COUNT(DISTINCT driver_id) FROM gold.fact_race_results {year_filter_fact};"
    ).fetchone()[0]

    total_constructors = con.execute(
        f"SELECT COUNT(DISTINCT constructor_id) FROM gold.fact_race_results {year_filter_fact};"
    ).fetchone()[0]

    con.close()

    return {
        "total_seasons": total_seasons,
        "total_races": total_races,
        "total_drivers": total_drivers,
        "total_constructors": total_constructors,
    }


def get_top_drivers(selected_years: list[int] | None = None, limit: int = 10) -> pl.DataFrame:
    """Top driver per punti totali, eventualmente filtrati per anni."""
    con = get_connection()

    if selected_years:
        years_tuple = tuple(selected_years)
        where = f"WHERE f.season_year IN {years_tuple}"
    else:
        where = ""

    sql = f"""
    SELECT
      d.forename || ' ' || d.surname AS driver_name,
      SUM(f.points) AS total_points
    FROM gold.fact_race_results f
    JOIN gold.dim_driver d ON d.driver_id = f.driver_id
    {where}
    GROUP BY driver_name
    ORDER BY total_points DESC
    LIMIT {limit};
    """

    df = con.execute(sql).pl()
    con.close()
    return df


def get_points_trend(selected_years: list[int] | None = None) -> pl.DataFrame:
    """Andamento dei punti medi per stagione."""
    con = get_connection()

    if selected_years:
        years_tuple = tuple(selected_years)
        where = f"WHERE season_year IN {years_tuple}"
    else:
        where = ""

    sql = f"""
    SELECT
      season_year,
      AVG(points) AS avg_points_per_result
    FROM gold.fact_race_results
    {where}
    GROUP BY season_year
    ORDER BY season_year;
    """

    df = con.execute(sql).pl()
    con.close()
    return df


# --------------------------------------------------
# TEXT-TO-SQL CON GEMINI
# --------------------------------------------------

def gemini_text_to_sql(question: str) -> str:
    if not genai or not GEMINI_API_KEY:
        raise RuntimeError("Gemini non configurato")

    prompt = f"""
    SEI UN ASSISTENTE SQL...
    ... (omesso per brevità) ...
    Domanda:
    \"\"\"{question}\"\"\"
    """

    model = genai.GenerativeModel("gemini-1.5-pro-latest")

    response = model.generate_content(prompt)
    sql = response.text.strip()

    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql

# --------------------------------------------------
# STREAMLIT APP
# --------------------------------------------------

def main():
    st.set_page_config(
        page_title="F1 Analytics – Data Engineering Project",
        layout="wide",
    )

    st.title("🏎️ F1 Analytics Dashboard")
    st.caption("Pipeline DuckDB + Prefect + Polars + Streamlit (GOLD layer)")

    # ---- SIDEBAR: FILTRI ----
    st.sidebar.header("Filtri")

    all_years = get_years()
    selected_years = st.sidebar.multiselect(
        "Seleziona una o più stagioni",
        options=all_years,
        default=all_years,  # per default: tutte
    )

    # Se l'utente seleziona tutti gli anni, per le query passo None e considero "no filtro"
    years_filter = None if set(selected_years) == set(all_years) else selected_years

    # ---- KPI SECTION ----
    st.subheader("Panoramica GOLD")

    kpis = get_kpis(years_filter)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stagioni", kpis["total_seasons"])
    c2.metric("Gare", kpis["total_races"])
    c3.metric("Piloti", kpis["total_drivers"])
    c4.metric("Costruttori", kpis["total_constructors"])

    # ---- TOP DRIVERS ----
    st.subheader("Top driver per punti totali")

    top_df = get_top_drivers(years_filter, limit=10)

    if top_df.height > 0:
        st.dataframe(top_df.to_pandas())

        chart_df = top_df.sort("total_points", descending=True)
        st.bar_chart(
            chart_df.to_pandas().set_index("driver_name")[["total_points"]]
        )
    else:
        st.info("Nessun dato disponibile per i filtri selezionati.")

    # ---- TREND PUNTI ----
    st.subheader("Andamento dei punti medi per stagione")

    trend_df = get_points_trend(years_filter)

    if trend_df.height > 0:
        st.dataframe(trend_df.to_pandas())
        st.line_chart(
            trend_df.to_pandas().set_index("season_year")[["avg_points_per_result"]]
        )
    else:
        st.info("Nessun dato disponibile per i filtri selezionati.")

    st.markdown("---")

    # ---- TEXT-TO-SQL CON GEMINI ----
    st.subheader("🧠 Text-to-SQL con Gemini (demo)")

    if not GEMINI_API_KEY:
        st.warning(
            "GEMINI_API_KEY non configurata. "
            "Aggiungi la chiave al file .env per attivare questa sezione."
        )
    else:
        st.write(
            "Scrivi una domanda in linguaggio naturale. "
            "Esempi: `migliori piloti nel 2010`, `classifica costruttori 2015`, "
            "`quante gare nel 2008`, `media punti per stagione`..."
        )

        user_question = st.text_input("Domanda")

        if user_question:
            with st.spinner("Genero la query SQL con Gemini..."):
                try:
                    sql = gemini_text_to_sql(user_question)
                except Exception as e:
                    st.error(f"Errore Gemini: {e}")
                    sql = None

            if sql:
                st.code(sql, language="sql")
                try:
                    con = get_connection()
                    result_df = con.execute(sql).pl()
                    con.close()

                    if result_df.height == 0:
                        st.info("La query non ha restituito risultati.")
                    else:
                        st.dataframe(result_df.to_pandas())

                        # Se la query restituisce un singolo valore numerico, mostriamo un metric
                        if result_df.height == 1 and result_df.width == 1:
                            value = result_df.row(0)[0]
                            st.metric("Risultato", value)
                except Exception as e:
                    st.error(f"Errore nell'esecuzione della query sul DB: {e}")

    st.markdown("---")
    st.caption(
        "La parte Text-to-SQL è opzionale: la dashboard principale rimane alimentata "
        "da query predefinite sul GOLD. Gemini viene usato solo per la funzionalità "
        "aggiuntiva di chat / interrogazione libera."
    )


if __name__ == "__main__":
    main()
