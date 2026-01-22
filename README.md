## Data Engineering Project – Formula 1

Questo repository contiene un progetto di Data Engineering end-to-end basato sui dati storici del campionato mondiale di Formula 1 (1950–2020).
Il progetto nasce con l’obiettivo di simulare un flusso dati realistico, dalla sorgente grezza fino al serving tramite dashboard, seguendo buone pratiche di data engineering.

Lo sviluppo è articolato in due fasi progressive. La prima fase costruisce una pipeline funzionante e una dashboard analitica di base. La seconda fase introduce un’evoluzione architetturale che rende la pipeline più robusta, automatizzata e vicina a uno scenario reale di produzione.

# Dataset

Il dataset utilizzato proviene da Kaggle ed è dedicato allo storico completo del campionato mondiale di Formula 1 dal 1950 al 2020.
Il dominio è quello del motorsport analytics e comprende le principali entità del campionato, come gare, piloti, costruttori e risultati. Questo tipo di dati si presta bene a esercizi di modellazione analitica, analisi temporali e costruzione di KPI.

# Obiettivo del progetto

L’obiettivo principale del progetto è costruire una pipeline dati completa e riproducibile che permetta di analizzare le performance di piloti e costruttori, studiare l’evoluzione delle stagioni nel tempo e supportare dashboard interattive e analisi esplorative.
Dal punto di vista architetturale, il progetto mira anche a simulare un contesto di data engineering realistico, introducendo concetti come stratificazione dei dati, ingestione incrementale, orchestrazione e scheduling automatico.

# Architettura a Layer

L’intero progetto è organizzato secondo un’architettura a layer, una scelta comune nei sistemi di data engineering moderni.

Il layer **Bronze** rappresenta il livello più vicino alla sorgente. Qui i dati vengono ingestiti in forma grezza, senza trasformazioni logiche, mantenendo una copia il più fedele possibile all’input originale.

Il layer **Silver** introduce una prima fase di trasformazione. In questo livello i dati vengono puliti, tipizzati correttamente, standardizzati nel naming e deduplicati. L’obiettivo è ottenere dataset consistenti e pronti per l’analisi.

Il layer **Gold** è pensato per il serving. In questa fase i dati vengono modellati secondo uno schema analitico a stella, con tabelle di fatto e dimensioni, ottimizzate per KPI, aggregazioni e dashboard.

# Fase 1 – Pipeline base e dashboard

La Fase 1 ha come obiettivo la costruzione di una pipeline end-to-end funzionante e di una prima dashboard analitica.

In questa fase i dati vengono ingestiti direttamente dai file CSV originali del dataset. Dopo l’ingestione, vengono applicate le trasformazioni necessarie per costruire il layer Silver, occupandosi di pulizia, normalizzazione dei tipi e coerenza dei nomi delle colonne.
Successivamente viene costruito il layer Gold, modellato in modo da supportare interrogazioni analitiche e KPI.

I dati vengono persistiti in **DuckDB**, che funge da data warehouse embedded, e la parte di serving è realizzata tramite una dashboard interattiva sviluppata con Streamlit.

Il risultato della Fase 1 è una pipeline completa e funzionante, con un modello Gold coerente e una dashboard locale in grado di visualizzare KPI e analisi. Tuttavia, l’ingestione dei dati è di tipo batch statico: a ogni esecuzione l’intero dataset viene rielaborato.

# Fase 2 – Data Lake, incrementalità e orchestrazione

La Fase 2 rappresenta un’evoluzione architetturale della pipeline, con l’obiettivo di simulare uno scenario più realistico di data engineering.

In questa fase viene introdotto un Data Lake open-source su filesystem locale, con dati salvati in formato Parquet, che diventa la fonte di verità della pipeline. I dati vengono suddivisi in batch giornalieri, organizzati per data di ingestione (dt=YYYY-MM-DD), in modo da simulare l’arrivo incrementale dei dati nel tempo.

L’ingestione diventa quindi incrementale: a ogni esecuzione la pipeline rileva quali batch sono già stati processati e quali no, grazie a un ledger di controllo (meta.processed_batches). Questo meccanismo garantisce idempotenza ed evita duplicazioni, permettendo di processare solo i nuovi dati.

L’intero flusso Bronze → Silver → Gold viene orchestrato tramite un flow unico, eseguibile sia in locale sia in modalità automatizzata.
Lo scheduling è gestito tramite GitHub Actions, che consente sia l’esecuzione manuale sia l’esecuzione schedulata via cron, simulando una pipeline automatica di produzione. Al termine di ogni run, il warehouse aggiornato viene versionato nel repository.

# Dashboard e serving

La dashboard Streamlit rimane collegata al layer Gold e continua a funzionare correttamente anche dopo più esecuzioni schedulate della pipeline. Questo dimostra che la parte di serving è completamente disaccoppiata dalla logica di ingestione.

La dashboard è stata inoltre deployata pubblicamente su Streamlit Cloud, rendendo i risultati consultabili senza necessità di esecuzione locale.
È presente anche una funzionalità opzionale di Text-to-SQL basata su Google Gemini, che consente interrogazioni in linguaggio naturale. Questa parte è accessoria e non influisce sul funzionamento principale della dashboard.

🔗 Dashboard pubblica:
https://data-engineering-project-f1.streamlit.app/

# Sicurezza e gestione delle credenziali

Il progetto non contiene alcuna API key nel codice o nel repository.
Le credenziali vengono gestite tramite st.secrets su Streamlit Cloud e tramite variabili d’ambiente in locale. La dashboard rimane pienamente operativa anche in assenza della chiave AI, garantendo una separazione netta tra funzionalità core e funzionalità opzionali.

# Tecnologie utilizzate

- **Python** – linguaggio principale del progetto 

- **DuckDB** – data warehouse embedded per la persistenza e le query analitiche  

- **Polars** – trasformazioni e manipolazione dei dati  

- **Streamlit** – serving e visualizzazione tramite dashboard interattiva  

- **GitHub Actions** – orchestrazione ed esecuzione schedulata della pipeline  

- **Parquet** – formato colonnare per lo storage nel Data Lake  

- **Google Gemini** – componente opzionale per funzionalità di Text-to-SQL

# Conclusione

Questo progetto dimostra la realizzazione di una pipeline di data engineering completa e stratificata, capace di gestire ingestione incrementale, orchestrazione automatica e serving tramite dashboard.

La Fase 2 non modifica i contenuti informativi della Fase 1, ma ne rappresenta un’evoluzione architetturale significativa, migliorando robustezza, scalabilità e automazione del sistema nel suo complesso.