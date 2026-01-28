## Data Engineering Project – Formula 1

Questo repository contiene un progetto di Data Engineering end-to-end basato sui dati storici del campionato mondiale di Formula 1 (1950–2024), esteso in una seconda fase con una simulazione di ingestione incrementale e orchestrata.

Il progetto nasce con l’obiettivo di simulare un flusso dati realistico, partendo da file CSV grezzi fino ad arrivare a un layer analitico interrogabile tramite dashboard Streamlit, seguendo buone pratiche di data engineering.

Il lavoro è strutturato in due fasi progressive:
la Fase 1, che costruisce una pipeline funzionante e una dashboard analitica di base, e la Fase 2, che introduce incrementalità, Data Lake, orchestrazione e scheduling automatico.

# Dataset

Il dataset utilizzato proviene da Kaggle – Formula 1 World Championship e copre l’intero storico del campionato dal 1950 al 2024.

Il dominio è quello del motorsport analytics e include le principali entità del campionato:
gare, piloti, costruttori, circuiti e risultati di gara.

# Obiettivo del progetto

L’obiettivo è costruire una pipeline dati completa e riproducibile che consenta di:

analizzare le performance di piloti e costruttori

studiare l’andamento delle stagioni nel tempo

supportare dashboard e analisi esplorative

simulare un’architettura di data engineering vicina a uno scenario reale

# Architettura a Layer

L’intero progetto è organizzato secondo un’architettura a layer logici, ispirata ai moderni data platform.

Il Bronze layer contiene i dati grezzi ingestiti dalla sorgente, senza trasformazioni logiche rilevanti.

Il Silver layer introduce pulizia, standardizzazione dei tipi, deduplicazione e naming coerente.

Il Gold layer espone un modello analitico a star schema, composto da fact table e dimension tables, pronto per KPI e dashboard.

# FASE 1 – Pipeline base e dashboard

La Fase 1 ha l’obiettivo di costruire una pipeline dati completa e funzionante, partendo dai CSV originali fino a una dashboard analitica.

In questa fase i dati vengono ingestiti in modalità batch “statica”: l’intero dataset viene ricaricato e trasformato a ogni esecuzione.

Tecnologie principali – Fase 1

Python

DuckDB (data warehouse embedded)

Polars (data processing)

Streamlit (serving e dashboard)

# Struttura della repository – Fase 1
data-engineering-project/
│
├── data/
│   ├── raw/                 # CSV originali (Kaggle)
│   ├── bronze/              # Tabelle Bronze
│   ├── silver/              # Tabelle Silver
│   ├── gold/                # Tabelle Gold
│   └── warehouse.duckdb     # DuckDB con Bronze/Silver/Gold
│
├── etl/
│   ├── bronze/              # Logica di ingestione
│   ├── silver/              # Trasformazioni e pulizia
│   └── gold/                # Modellazione analitica
│
├── dashboard/
│   └── app.py               # Streamlit dashboard
│
├── requirements.txt
└── README.md

# Output Fase 1

Al termine della Fase 1 il progetto include:

una pipeline end-to-end funzionante

un modello Gold coerente

una dashboard Streamlit con KPI e visualizzazioni

Questa fase rappresenta una baseline funzionale, ma non prevede incrementalità né automazione.

## FASE 2 – Data Lake, incrementalità e orchestrazione

La Fase 2 introduce un’evoluzione architetturale significativa, trasformando la pipeline in uno scenario più realistico e vicino a un contesto di produzione.

In questa fase viene introdotto un Data Lake, l’ingestione incrementale, un ledger di controllo, l’orchestrazione completa del flusso e lo scheduling automatico.

# Data Lake

Il Data Lake è implementato su filesystem locale e utilizza Parquet come formato colonnare.

I dati sono organizzati in batch giornalieri, partizionati per data (dt=YYYY-MM-DD), che simulano l’arrivo progressivo dei dati.

data_lake/
└── raw/
    └── dt=YYYY-MM-DD/
        ├── races.parquet
        ├── results.parquet
        └── ...


Il Data Lake diventa la fonte di verità della pipeline in Fase 2.

# Generazione di dati sintetici

Per rendere la pipeline realmente incrementale, viene introdotto uno script di generazione di stagioni sintetiche.

Ad ogni run:

viene creata una nuova stagione F1 (es. 2025, 2026, …)

il numero di gare e piloti rimane coerente con una stagione reale

i punti e i risultati vengono randomizzati

la stagione viene generata solo se non è già presente

La generazione è realizzata tramite Python + Polars, senza librerie di simulazione esterne.

# Ingestione incrementale e Ledger

L’ingestione Bronze utilizza un ledger di controllo (meta.processed_batches) che registra i batch già processati.

Ad ogni run la pipeline:

legge i batch presenti nel Data Lake

confronta le date con il ledger

processa solo i nuovi batch

garantisce idempotenza e assenza di duplicati

# Orchestrazione e Scheduling

La pipeline Fase 2 è orchestrata come flow unico:

Bronze → Silver → Gold


L’esecuzione avviene tramite:

run locale

GitHub Actions

run manuale

run schedulato giornaliero (cron)

Al termine di ogni run:

il DuckDB aggiornato viene committato

i nuovi batch del Data Lake vengono versionati (solo per demo)

# Dashboard e Serving

La dashboard Streamlit rimane invariata rispetto alla Fase 1 e continua a interrogare il Gold layer.

La dashboard è:

deployata pubblicamente su Streamlit Cloud

compatibile con più run incrementali

indipendente dalla presenza della componente AI

La funzionalità Text-to-SQL basata su Google Gemini è opzionale ed è attivata solo se la chiave API è presente nei Secrets.

# Struttura della repository – Dopo Fase 2
data-engineering-project/
│
├── data/
│   └── warehouse.duckdb        # Snapshot DuckDB per Streamlit Cloud
│
├── data_lake/
│   └── raw/
│       └── dt=YYYY-MM-DD/      # Batch incrementali (Parquet)
│
├── etl/
│   ├── tasks/
│   │   ├── bronze_fase2.py
│   │   ├── silver_fase2.py
│   │   └── gold_fase2.py
│   ├── flows/
│   │   └── fase2_flow.py
│   └── utils.py
│
├── scripts/
│   └── generate_synthetic_season_batch.py
│
├── dashboard/
│   └── app.py
│
├── .github/
│   └── workflows/
│       └── fase2_pipeline.yml
│
├── requirements.txt
├── runtime.txt
└── README.md

# Conclusione

Questo progetto dimostra una pipeline dati completa e stratificata che evolve da una soluzione batch semplice a un’architettura incrementale, orchestrata e schedulata.

La Fase 2 non modifica il contenuto informativo della Fase 1, ma ne migliora in modo significativo:

robustezza

automazione

realismo architetturale

riproducibilità

Il risultato è una pipeline coerente che integra Data Engineering e Data Visualization in un flusso unico e dimostrabile.

# Dashboard pubblica:
https://data-engineering-project-f1.streamlit.app/