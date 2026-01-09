from pathlib import Path

import duckdb
import polars as pl
import streamlit as st
from etl.utils import get_connection

# --------------------------------------------------
# TEXT-TO-SQL (versione rule-based)
# --------------------------------------------------

def text_to_sql(question: str) -> str | None:
    """
    Mappa alcune domande in linguaggio naturale a query SQL sul layer GOLD.
    Versione semplice ma spiegabile bene in un progetto didattico/aziendale.

    Esempi supportati:
    - "migliori piloti"
    - "migliori piloti nel 2010"
    - "classifica costruttori 2015"
    - "quante gare nel 2005"
    - "punti medi per stagione"
    """
    q = question.lower().strip()

    # estrai un anno (tipo 1999, 2010, 2020...)
    match_year = re.search(r"(19|20)\d{2}", q)
    year = match_year.group(0) if match_year else None

    # 1) migliori piloti per punti totali (con anno opzionale)
    if ("migliori piloti" in q or "top driver" in q or "top piloti" in q) and "costruttori" not in q:
        if year:
            return f"""
            SELECT
              d.driver_id,
              d.forename || ' ' || d.surname AS driver_name,
              SUM(f.points) AS total_points
            FROM gold.fact_race_results f
            JOIN gold.dim_driver d ON d.driver_id = f.driver_id
            WHERE f.season_year = {year}
            GROUP BY 1, 2
            ORDER BY total_points DESC
            LIMIT 10;
            """
        else:
            return """
            SELECT
              d.driver_id,
              d.forename || ' ' || d.surname AS driver_name,
              SUM(f.points) AS total_points
            FROM gold.fact_race_results f
            JOIN gold.dim_driver d ON d.driver_id = f.driver_id
            GROUP BY 1, 2
            ORDER BY total_points DESC
            LIMIT 10;
            """

    # 2) classifica costruttori
    if "costruttori" in q or "constructor" in q:
        if year:
            return f"""
            SELECT
              c.constructor_id,
              c.constructor_name,
              SUM(f.points) AS total_points
            FROM gold.fact_race_results f
            JOIN gold.dim_constructor c ON c.constructor_id = f.constructor_id
            WHERE f.season_year = {year}
            GROUP BY 1, 2
            ORDER BY total_points DESC
            LIMIT 10;
            """
        else:
            return """
            SELECT
              c.constructor_id,
              c.constructor_name,
              SUM(f.points) AS total_points
            FROM gold.fact_race_results f
            JOIN gold.dim_constructor c ON c.constructor_id = f.constructor_id
            GROUP BY 1, 2
            ORDER BY total_points DESC
            LIMIT 10;
            """

    # 3) quante gare (per anno o per tutti gli anni)
    if "quante gare" in q or "numero gare" in q or "gare disputate" in q:
        if year:
            return f"""
            SELECT
              season_year,
              COUNT(DISTINCT race_id) AS num_races
            FROM gold.dim_race
            WHERE season_year = {year}
            GROUP BY season_year;
            """
        else:
            return """
            SELECT
              season_year,
              COUNT(DISTINCT race_id) AS num_races
            FROM gold.dim_race
            GROUP BY season_year
            ORDER BY season_year;
            """

    # 4) punti medi per stagione
    if "punti medi" in q or "media punti" in q:
        return """
        SELECT
          season_year,
          AVG(points) AS avg_points_per_result
        FROM gold.fact_race_results
        GROUP BY season_year
        ORDER BY season_year;
        """

    # nessuna regola trovata
    return None

#  (KPI E DATAFRAME)

def get_years():
    con = get_connection()
    years = con.execute(
        "SELECT DISTINCT season_year FROM gold.dim_race ORDER BY season_year;"
    ).fetchall()
    con.close()
    return [y[0] for y in years]


def get_kpis(selected_years: list[int] | None = None) -> dict:
    """
    Restituisce qualche KPI base in funzione del filtro anni.
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



# STREAMLIT APP


def main():
    st.set_page_config(
        page_title="F1 Analytics – Data Engineering Project",
        layout="wide",
    )

    st.title("🏎️ F1 Analytics Dashboard")
    st.caption("Pipeline DuckDB + Prefect + Polars + Streamlit (Gold layer)")

    # ---- SIDEBAR: FILTRI ----
    st.sidebar.header("Filtri")

    all_years = get_years()
    selected_years = st.sidebar.multiselect(
        "Seleziona una o più stagioni",
        options=all_years,
        default=all_years,  # di default tutte
    )

    # ---- KPI SECTION ----
    st.subheader("Panoramica")

    kpis = get_kpis(selected_years if selected_years != all_years else None)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stagioni", kpis["total_seasons"])
    c2.metric("Gare", kpis["total_races"])
    c3.metric("Piloti", kpis["total_drivers"])
    c4.metric("Costruttori", kpis["total_constructors"])

    # ---- TOP DRIVERS ----
    st.subheader("Top driver per punti totali")

    top_df = get_top_drivers(selected_years if selected_years != all_years else None, limit=10)

    if top_df.height > 0:
        st.dataframe(top_df.to_pandas())

        # bar chart
        chart_df = top_df.sort("total_points", descending=True)
        st.bar_chart(
            chart_df.to_pandas().set_index("driver_name")[["total_points"]]
        )
    else:
        st.info("Nessun dato disponibile per i filtri selezionati.")

    # ---- TREND PUNTI ----
    st.subheader("Andamento dei punti medi per stagione")

    trend_df = get_points_trend(selected_years if selected_years != all_years else None)

    if trend_df.height > 0:
        st.dataframe(trend_df.to_pandas())
        st.line_chart(
            trend_df.to_pandas().set_index("season_year")[["avg_points_per_result"]]
        )
    else:
        st.info("Nessun dato disponibile per i filtri selezionati.")

    st.markdown("---")

    # ---- TEXT-TO-SQL MODULE ----
    st.subheader("🧠 Modulo Text-to-SQL (demo)")

    st.write(
        "Scrivi una domanda in linguaggio naturale. "
        "Esempi: "
        "`migliori piloti nel 2010`, `classifica costruttori 2015`, "
        "`quante gare nel 2005`, `punti medi per stagione`."
    )

    question = st.text_input("Domanda")

    if question:
        sql = text_to_sql(question)
        if sql is None:
            st.warning(
                "Domanda non riconosciuta. Prova con: "
                "“migliori piloti nel 2010”, “classifica costruttori 2015”, "
                "“quante gare nel 2005”, “punti medi per stagione”…"
            )
        else:
            st.code(sql, language="sql")
            try:
                con = get_connection()
                result_df = con.execute(sql).pl()
                con.close()

                if result_df.height == 0:
                    st.info("La query non ha restituito risultati.")
                else:
                    st.dataframe(result_df.to_pandas())

                    # se c'è season_year, proviamo a fare un grafico
                    if "season_year" in result_df.columns and result_df.height > 1:
                        st.line_chart(
                            result_df.to_pandas().set_index("season_year").select_dtypes("number")
                        )
            except Exception as e:
                st.error(f"Errore nell'esecuzione della query: {e}")

    st.markdown("---")
    st.caption(
        "Questo modulo è una versione semplificata di text-to-SQL: "
        "rule-based, nessun modello esterno, pensata per essere estesa in futuro."
    )


if __name__ == "__main__":
    main()
