import sys
from pathlib import Path
import os
import re

import polars as pl
import streamlit as st

# --- PATH: aggiungo la root del progetto al PYTHONPATH ---
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from etl.utils import get_connection


# =========================
# GEMINI / TEXT-TO-SQL SETUP
# =========================

def get_gemini_api_key() -> str | None:
    """
    Ordine di priorità:
    1) Streamlit Cloud secrets
    2) Variabili d'ambiente (locale/CI)
    """
    try:
        # se secrets.toml è invalido in locale, Streamlit può lanciare eccezione
        if "GEMINI_API_KEY" in st.secrets:
            return st.secrets["GEMINI_API_KEY"]
    except Exception:
        pass

    return os.getenv("GEMINI_API_KEY")


@st.cache_resource(show_spinner=False)
def init_gemini() -> tuple[object | None, str | None]:
    """
    Inizializza Gemini una sola volta (cache Streamlit).
    Ritorna:
      - genai module (o None se non configurabile)
      - nome modello (o None)
    """
    api_key = get_gemini_api_key()
    if not api_key:
        return None, None

    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)

        # Seleziona un modello che supporta generateContent
        models = list(genai.list_models())
        gc_models = [
            m for m in models
            if "generateContent" in getattr(m, "supported_generation_methods", [])
        ]

        preferred_suffixes = [
            "gemini-1.5-pro",
            "gemini-1.5-flash",
            "gemini-pro",
        ]

        chosen = None
        for suffix in preferred_suffixes:
            for m in gc_models:
                if m.name.endswith(suffix):
                    chosen = m.name
                    break
            if chosen:
                break

        if not chosen and gc_models:
            chosen = gc_models[0].name

        if not chosen:
            return None, None

        return genai, chosen

    except Exception:
        # Non blocchiamo la dashboard se Gemini non va
        return None, None


genai, GEMINI_MODEL_NAME = init_gemini()


# =========================
# DUCKDB
# =========================

def get_duckdb_connection():
    return get_connection(read_only=True)


# =========================
# FUNZIONI DI ACCESSO AL GOLD
# =========================

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


# =========================
# TEXT-TO-SQL CON GEMINI
# =========================

def _clean_sql(sql: str) -> str:
    sql = sql.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql


def _is_safe_select(sql: str) -> bool:
    """
    Guardrail minimale:
    - ammettiamo solo SELECT (niente INSERT/UPDATE/DELETE/CREATE/DROP)
    """
    s = re.sub(r"\s+", " ", sql.strip().lower())
    if not s.startswith("select"):
        return False

    forbidden = ["insert", "update", "delete", "create", "drop", "alter", "attach", "copy", "pragma"]
    return not any(f" {kw} " in f" {s} " for kw in forbidden)


def gemini_text_to_sql(question: str) -> str:
    """
    Usa Gemini per tradurre una domanda in linguaggio naturale
    in una query SQL sullo schema GOLD (DuckDB).
    """
    if genai is None or GEMINI_MODEL_NAME is None:
        raise RuntimeError(
            "Gemini non configurato: aggiungi GEMINI_API_KEY nei Secrets (Streamlit Cloud) o come env var (locale)."
        )

    prompt = f"""
Sei un assistente che converte domande in SQL per DuckDB.

Schema F1 (namespace GOLD):

- gold.fact_race_results(
    race_id, driver_id, constructor_id,
    season_year,
    grid, position, points, laps, status_id
  )

- gold.dim_driver(
    driver_id, forename, surname, driver_ref, nationality
  )

- gold.dim_constructor(
    constructor_id, constructor_name, nationality
  )

- gold.dim_race(
    race_id, season_year, round, race_name, circuit_id, race_date
  )

Regole IMPORTANTI:
- Genera SOLO SQL, nessun commento, nessuna spiegazione.
- NON usare SELECT *.
- Usa sempre il namespace gold.<tabella>.
- Assicurati che i nomi delle colonne esistano nello schema.
- Se l'utente non specifica l'anno, usa tutte le stagioni.
- Limita il risultato a 50 righe quando ha senso.
- SQL deve essere compatibile con DuckDB.

Domanda utente:
\"\"\"{question}\"\"\"
"""

    model = genai.GenerativeModel(GEMINI_MODEL_NAME)
    response = model.generate_content(prompt)
    sql = _clean_sql(response.text)

    if not _is_safe_select(sql):
        raise RuntimeError("La query generata non è consentita (sono ammessi solo SELECT).")

    return sql


# =========================
# STREAMLIT APP
# =========================

def main():
    st.set_page_config(
        page_title="F1 Analytics – Data Engineering Project",
        layout="wide",
    )

    st.title("🏎️ F1 Analytics Dashboard")
    st.caption("Pipeline DuckDB + Prefect + Polars + Streamlit (GOLD layer)")

    # SIDEBAR: FILTRI
    st.sidebar.header("Filtri")

    all_years = get_years()
    selected_years = st.sidebar.multiselect(
        "Seleziona una o più stagioni",
        options=all_years,
        default=all_years,
    )

    years_filter = None if set(selected_years) == set(all_years) else selected_years

    # KPI SECTION
    st.subheader("Panoramica GOLD")

    kpis = get_kpis(years_filter)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Stagioni", kpis["total_seasons"])
    c2.metric("Gare", kpis["total_races"])
    c3.metric("Piloti", kpis["total_drivers"])
    c4.metric("Costruttori", kpis["total_constructors"])

    # TOP DRIVERS
    st.subheader("Top driver per punti totali")

    top_df = get_top_drivers(years_filter, limit=10)

    if top_df.height > 0:
        # più largo verso destra: mettiamo la tabella in una colonna “grassa”
        left, right = st.columns([3, 1])
        with left:
            st.dataframe(top_df.to_pandas(), use_container_width=True)

        chart_df = top_df.sort("total_points", descending=True)
        st.bar_chart(chart_df.to_pandas().set_index("driver_name")[["total_points"]])
    else:
        st.info("Nessun dato disponibile per i filtri selezionati.")

    # TREND PUNTI
    st.subheader("Andamento dei punti medi per stagione")

    trend_df = get_points_trend(years_filter)

    if trend_df.height > 0:
        # evita 1,950 -> cast a string (anno non è una quantità)
        trend_df_display = trend_df.with_columns(
            pl.col("season_year").cast(pl.Utf8)
        )

        st.dataframe(trend_df_display.to_pandas(), use_container_width=True)

        # per il chart: usiamo pandas con season_year come stringa (niente separatore migliaia)
        pdf_trend = trend_df.to_pandas()
        pdf_trend["season_year"] = pdf_trend["season_year"].astype(str)

        st.line_chart(pdf_trend.set_index("season_year")[["avg_points_per_result"]])
    else:
        st.info("Nessun dato disponibile per i filtri selezionati.")

    st.markdown("---")

    # TEXT-TO-SQL CON GEMINI
    st.subheader("Text-to-SQL con Gemini")

    if genai is None or GEMINI_MODEL_NAME is None:
        st.warning(
            "Text-to-SQL disabilitato: manca GEMINI_API_KEY nei Secrets (Streamlit Cloud) "
            "o come variabile d’ambiente in locale."
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
                        st.dataframe(result_df.to_pandas(), use_container_width=True)

                        if result_df.height == 1 and result_df.width == 1:
                            value = result_df.row(0)[0]
                            st.metric("Risultato", value)
                except Exception as e:
                    st.error(f"Errore nell'esecuzione della query sul DB: {e}")

    st.markdown("---")
    st.caption(
        "La parte Text-to-SQL è opzionale: la dashboard principale rimane alimentata "
        "da query predefinite sul GOLD. Gemini viene usato solo per la funzionalità "
        "aggiuntiva di interrogazione libera."
    )


if __name__ == "__main__":
    main()
