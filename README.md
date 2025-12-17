# data-engineering project

# Data Engineering Project – Formula 1

Questo repository contiene un progetto di **Data Engineering end-to-end** basato sui dati storici del campionato mondiale di **Formula 1 (1950–2020)**.

Il progetto copre l’intero flusso:
- ingestione dei dati
- modellazione
- orchestrazione
- serving tramite dashboard

---

# Obiettivo
Costruire una pipeline dati completa e riproducibile che consenta di:
- analizzare performance di piloti e costruttori
- studiare l’andamento delle stagioni nel tempo
- supportare dashboard e analisi esplorative

---

## Architettura a Layer

- **Bronze**  
  Dati grezzi ingestiti dai file CSV originali.

- **Silver**  
  Dati puliti e standardizzati (tipi, naming, gestione valori sporchi).

- **Gold**  
  Modello analitico a **star schema** (fact & dimension tables), pronto per dashboard e KPI.

Il design del modello dati è documentato in:
