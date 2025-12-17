# Formula 1 – Star Schema 

## Contesto del progetto
Questo documento descrive lo **star schema completo** progettato per il dataset
*Formula 1 World Championship (1950–2020)*.

Lo schema è stato definito **prima dell’implementazione del Silver e Gold layer**
per chiarire:
- il modello analitico finale
- la granularità dei dati
- le relazioni tra fatti e dimensioni

L’obiettivo è costruire una base dati **orientata all’analisi**, facilmente
interrogabile da dashboard e strumenti di BI.

---

## Architettura generale dei layer

- **Bronze**  
  Contiene i dati grezzi così come arrivano dalle sorgenti CSV.
  Non viene applicata alcuna logica di business.

- **Silver**  
  Contiene dati puliti e standardizzati:
  - tipi di dato coerenti
  - colonne rinominate
  - gestione dei valori sporchi  
  In questo layer **non** sono presenti KPI o aggregazioni.

- **Gold**  
  Contiene lo **star schema** (fact + dimension tables).
  È il layer utilizzato direttamente per analisi e dashboard.

Questo documento si riferisce **esclusivamente al Gold layer**.

---

## Obiettivi di Business

Le principali domande a cui il modello deve rispondere sono:

- Analizzare le **performance dei piloti** nel tempo
- Analizzare le **performance dei team/costruttori**
- Confrontare **qualifica vs risultato finale**
- Analizzare l’impatto di **pit stop** e **strategie**
- Studiare il **passo gara** tramite lap times
- Analizzare l’andamento del **campionato gara dopo gara**

---

## KPI principali supportati

Alcuni esempi di KPI che possono essere calcolati facilmente sul modello:

- Punti totali per pilota / team
- Numero di vittorie e podi
- Posizione media di arrivo
- Tasso di DNF
- Differenza tra posizione di partenza e arrivo
- Numero medio di pit stop
- Durata media dei pit stop
- Miglior giro e passo medio
- Trend dei punti nel campionato

---

## Grain del modello

La decisione principale riguarda la granularità della fact table centrale.

### Grain della fact principale
**1 riga = 1 pilota in 1 gara**

Questa granularità consente di:
- calcolare KPI semplici e complessi
- aggregare facilmente per pilota, team o stagione
- evitare duplicazioni logiche

---

## Fact Tables

### fact_race_results (fact centrale)

**Descrizione**  
Contiene il risultato finale di ogni pilota in ogni gara.
È la fact table principale dello schema.

**Chiavi (Foreign Key)**
- `race_id`
- `date_id`
- `driver_id`
- `constructor_id`
- `status_id`

**Misure principali**
- `points`
- `position`
- `position_order`
- `grid`
- `laps`
- `milliseconds`

**Flag derivati**
- `is_win`
- `is_podium`
- `is_dnf`

> Nota personale: questa tabella è il cuore dello schema.
> La maggior parte delle analisi e delle dashboard parte da qui.

---

### fact_qualifying

**Grain**  
1 pilota in 1 gara (fase di qualifica).

**Misure**
- `qualifying_position`
- `q1_ms`
- `q2_ms`
- `q3_ms`

Questa tabella viene utilizzata per confrontare
la posizione in griglia con il risultato finale in gara.

---

### fact_pit_stops

**Grain**  
1 riga = 1 pit stop.

**Misure**
- `stop_number`
- `lap`
- `pit_duration_ms`

Questa fact permette di analizzare strategie di gara
e l’impatto dei pit stop sulle performance.

---

### fact_lap_times

**Grain**  
1 riga = 1 giro completato da un pilota.

**Misure**
- `lap_number`
- `lap_time_ms`

> Nota personale: questa è una tabella molto grande.
> È pensata per analisi avanzate sul passo gara,
> non per dashboard di alto livello.

---

### fact_driver_standings

**Grain**  
1 pilota dopo ogni gara.

**Misure**
- `championship_points`
- `championship_position`
- `wins_to_date`

Serve per analizzare l’evoluzione del campionato nel tempo.

---

### fact_constructor_standings

**Grain**  
1 costruttore dopo ogni gara.

**Misure**
- `championship_points`
- `championship_position`
- `wins_to_date`

---

## Dimension Tables

### dim_driver

**Descrizione**  
Anagrafica dei piloti.

**Attributi principali**
- nome e cognome
- data di nascita
- nazionalità
- codice pilota
- numero di gara

---

### dim_constructor

**Descrizione**  
Anagrafica dei team/costruttori.

**Attributi**
- nome costruttore
- nazionalità

---

### dim_race

**Descrizione**  
Rappresenta l’evento gara.

**Attributi**
- stagione
- round
- nome della gara
- data della gara
- riferimento al circuito

---

### dim_circuit

**Descrizione**  
Informazioni geografiche sui circuiti.

**Attributi**
- nome circuito
- città
- paese
- coordinate geografiche

---

### dim_date

**Descrizione**  
Dimensione calendario.

**Attributi**
- data
- anno
- mese
- trimestre
- giorno della settimana
- flag weekend

Questa dimensione è generata manualmente
per supportare analisi temporali corrette.

---

### dim_status

**Descrizione**  
Motivo di fine gara (Finished, Accident, Engine, ecc.).

---

## Considerazioni finali

Lo star schema è stato progettato per:
- essere facilmente estendibile
- separare chiaramente fatti e dimensioni
- supportare sia analisi semplici sia avanzate

La separazione Bronze → Silver → Gold
permette di mantenere il modello pulito,
versionabile e facilmente manutenibile.

---

