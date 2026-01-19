# data-engineering project

## Data Engineering Project – Formula 1

Questo repository contiene un progetto di Data Engineering end-to-end basato sui dati storici del campionato mondiale di Formula 1 (1950–2020).

Il progetto è sviluppato in due fasi progressive, con l’obiettivo di costruire una pipeline dati completa, riproducibile e pronta per il serving tramite dashboard.

# Dataset

Fonte: Kaggle – Formula 1 World Championship (1950–2020)

Dominio: motorsport / analytics

Entità principali: gare, piloti, costruttori, risultati

# Obiettivo del progetto

Costruire una pipeline dati che consenta di:

analizzare le performance di piloti e costruttori

studiare l’andamento delle stagioni nel tempo

supportare dashboard interattive e analisi esplorative

simulare un’architettura data engineering realistica

# Architettura a Layer

L’intero progetto è organizzato secondo un’architettura a layer:

Bronze
Dati grezzi ingestiti dalla sorgente, senza trasformazioni logiche.

Silver
Dati puliti e standardizzati (tipi, naming coerente, deduplicazione).

Gold
Modello analitico a star schema (fact & dimension tables), pronto per KPI e dashboard.

# FASE 1 – Pipeline base e dashboard

La Fase 1 ha l’obiettivo di costruire una pipeline funzionante e una prima dashboard analitica.

Caratteristiche principali

Ingestione dei dati dai CSV originali

Trasformazioni Silver (pulizia e normalizzazione)

Costruzione del layer Gold

Persistenza su DuckDB

Dashboard interattiva con Streamlit

# Tecnologie utilizzate

Python

DuckDB

Polars

Streamlit

Output Fase 1

Pipeline funzionante end-to-end

Modello Gold coerente

Dashboard locale con KPI e visualizzazioni

Questa fase rappresenta una baseline funzionale: i dati sono corretti e la dashboard è operativa, ma l’ingestione è batch “statica”.

# FASE 2 – Data Lake, incrementalità e orchestrazione

La Fase 2 introduce un’evoluzione architetturale, simulando uno scenario più realistico di data engineering.

Novità introdotte

Data Lake open-source su filesystem locale

Suddivisione dei dati per batch giornalieri (dt=YYYY-MM-DD)

Ingestione incrementale

Ledger di controllo per evitare duplicazioni

Orchestrazione completa del flusso

Scheduling automatico

# Data Lake

Formato: Parquet (colonnare)

Struttura:

data_lake/raw/
  └── dt=YYYY-MM-DD/
       ├── races.parquet
       ├── drivers.parquet
       ├── results.parquet
       └── ...

Il Data Lake diventa la fonte di verità della pipeline.

# Ingestione Incrementale

Ogni esecuzione della pipeline:

rileva i batch presenti nel Data Lake

confronta le date con il ledger (meta.processed_batches)

processa solo i nuovi dati

garantisce idempotenza e coerenza storica

Orchestrazione e Scheduling

Flow unico Bronze → Silver → Gold

Esecuzione locale

# GitHub Actions:

run manuale

run schedulato via cron

Commit automatico del warehouse aggiornato

Questo consente di simulare una pipeline automatizzata e ripetibile.

Dashboard e Serving

Dashboard Streamlit collegata al layer Gold

Deploy pubblico su Streamlit Cloud

La dashboard continua a funzionare correttamente anche dopo più run schedulate della pipeline

La parte Text-to-SQL con Gemini è opzionale e gestita tramite Secrets

# Sicurezza e Secrets

Nessuna API key nel codice o nel repository

Gestione credenziali tramite:

st.secrets su Streamlit Cloud

variabili d’ambiente in locale

La dashboard rimane operativa anche senza chiave AI

Tecnologie principali

Python

DuckDB

Polars

Streamlit

GitHub Actions

Parquet

(Opzionale) Google Gemini – Text-to-SQL

# Conclusione

Il progetto dimostra:

una pipeline dati completa e stratificata

ingestione incrementale realistica

orchestrazione e scheduling automatico

integrazione tra data engineering e data visualization

La Fase 2 rappresenta un’evoluzione architetturale della Fase 1, senza modificare i contenuti informativi, ma migliorando robustezza, scalabilità e automazione.