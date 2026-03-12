# 2008-crisis-research

Projeto de pesquisa para explorar dados de hipotecas e s?ries macroecon?micas relacionadas ? crise de 2008.

## Requisitos
- Python 3.10+

## Setup r?pido
```bash
python -m venv .venv
source .venv/bin/activate  # no Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Estrutura de pastas
- `data/raw/` dados brutos baixados (por data)
- `data/staging/` dados intermedi?rios
- `data/processed/` datasets prontos para treino
- `src/` c?digo de ingest?o, ETL, features e modelos
- `notebooks/` an?lises explorat?rias
- `mlflow/` tracking local do MLflow

## Vari?veis de ambiente
Algumas fontes podem exigir autentica??o. Use vari?veis de ambiente:
- `FREDDIE_USER` / `FREDDIE_PASS`
- `FANNIE_USER` / `FANNIE_PASS`
- `FRED_API_KEY`

## Ingest?o
Cada script salva arquivos em `data/raw/YYYYMMDD/`.

```bash
python -m src.ingestion.download_freddie --url "https://.../sample.zip"
python -m src.ingestion.download_fannie --url "https://.../sample.zip"
python -m src.ingestion.download_fred --series "MORTGAGE30US" --start 2000-01-01
```

## ETL
Converte CSVs brutos para Parquet particionado por ano/m?s e exporta dataset de treino em `data/processed/`.

```bash
python -m src.etl.transform_loans --input data/raw --output data/staging
python -m src.etl.aggregate_macro --input data/raw --output data/staging
python -m src.features.build_features --input data/staging --output data/processed
```

## Treino baseline
Treina um modelo (XGBoost ou LightGBM) e registra no MLflow local.

```bash
mlflow ui --backend-store-uri ./mlflow
python -m src.models.train_baseline --data data/processed
```

## Avalia??o
```bash
python -m src.models.evaluate --data data/processed
```

## Notebooks
Abra com Jupyter:
```bash
jupyter lab
```
- `notebooks/01_exploratory.ipynb`
- `notebooks/02_model_baseline.ipynb`

## Testes e lint
```bash
pytest
flake8
```

## Architecture (high level)
- Ingestion: `src/pipeline/pipeline_ingest.py`
- Validation: Great Expectations checks inside pipeline steps
- Transformation: `src/pipeline/pipeline_transform.py`
- Feature engineering: `src/pipeline/pipeline_features.py` and `src/features/risk_features.py`
- Training: `src/pipeline/pipeline_training.py` and `src/models/`
- Systemic risk: `src/systemic_risk/`
- Time series: `src/timeseries/`
- Security and governance: `src/security/`
- MLOps: `mlops/`
- Dashboard: `dashboard/app.py`

## Pipelines (advanced)
```bash
python -m src.pipeline.pipeline_ingest --fred-series "MORTGAGE30US" --start 2000-01-01 --validate
python -m src.pipeline.pipeline_transform --input data/raw --output data/staging --validate
python -m src.pipeline.pipeline_features --staging data/staging --processed data/processed --validate
python -m src.pipeline.pipeline_training --processed data/processed --mlflow ./mlflow --out ./models
```

## Systemic risk analysis
Use `src/systemic_risk/` modules to build a network from exposures and simulate contagion.

## Time series forecasting
```bash
python -m src.timeseries.macro_forecasting --input data/staging/macro.parquet --target-col value --model arima
```

## MLOps helpers
```bash
python -m mlops.train_pipeline --processed data/processed --mlflow ./mlflow --out ./models
python -m mlops.register_model --model-path models/xgboost_target_default.json --name xgb_default
python -m mlops.model_monitoring --data data/processed/features.parquet
```

## Dashboard
```bash
streamlit run dashboard/app.py
```

## GLOBAL FINANCIAL INTELLIGENCE PLATFORM
This repository now includes a global monitoring architecture focused on stability, fraud detection, and systemic risk.

### Architecture diagram (high level)
```mermaid
flowchart LR
  A[Global Data Sources] --> B[Global Data Ingestion]
  B --> C[Global Data Lake]
  C --> D[Risk Engine]
  C --> E[Fraud and Anomaly Detection]
  D --> F[Systemic Risk Models]
  E --> G[Alerts and Monitoring]
  F --> G
  G --> H[Global Dashboard]
```

### Data sources
- Central bank and macro indicators (World Bank, FRED)
- Financial market indices (mock connectors)
- Housing and credit market series (public indicators)

### AI models
- Classical ML: Isolation Forest, XGBoost, LightGBM
- Neural nets: Autoencoder, LSTM, Temporal Fusion Transformer
- Survival analysis: Cox model

### Risk analysis methodology
- Institution, sector, country and global risk tiers
- Network centrality and exposure based systemic score
- Macro risk indices from indicator aggregation

### Simulation capabilities
- Stress scenarios (liquidity, credit, housing, sovereign)
- Multi-agent economic behavior simulation
- Contagion and exposure propagation

### Global modules (entry points)
```bash
python -m src.global_data.global_data_ingestion --fred-series "MORTGAGE30US" --wb-indicators "NY.GDP.MKTP.CD"
python -m mlops.data_pipeline --mock-market
streamlit run dashboard/global_dashboard.py
```

## Financial Crisis Simulator
This layer provides early warning signals, agent-based simulations, macro shock generation and systemic risk propagation.

### Architecture diagram (simulator)
```mermaid
flowchart LR
  A[Macro and Mortgage Data] --> B[Early Warning Indicators]
  B --> C[Crisis Probability Model]
  C --> D[Simulation Engine]
  D --> E[Systemic Risk Propagation]
  E --> F[Dashboard]
```

### Build financial dataset and early warning signals
```bash
python -m src.early_warning.risk_indicator_builder --loans data/staging/loans.parquet --macro data/staging/macro.parquet --hpi data/staging/housing_price.parquet --market data/staging/market.parquet
```

### Data sources (real data wiring)
Configure indicator sources in `config/early_warning_sources.json` and then run:
```bash
python -m mlops.financial_dataset_pipeline --config config/early_warning_sources.json --country all
```

### Knowledge graph queries (Neo4j)
Use `src/knowledge_graph/queries.py` for predefined Cypher queries and run via `run_queries` in `src/knowledge_graph/graph_builder.py`.

### Train early warning models
```bash
python -c "import pandas as pd; from src.early_warning.early_warning_model import train_early_warning_models; df=pd.read_parquet('data/financial_dataset.parquet'); df['target']=0; print(train_early_warning_models(df,'target'))"
```

### Crisis probability scoring
```bash
python -c "import pandas as pd; from src.early_warning.crisis_probability_model import build_crisis_probability; df=pd.read_parquet('data/financial_dataset.parquet'); df['target']=0; out=build_crisis_probability(df,'target'); out.to_parquet('data/financial_dataset.parquet', index=False)"
```

### Run simulations
```bash
python -m mlops.simulation_pipeline --out data/simulation_results.parquet
```

### Dashboard
```bash
streamlit run dashboard/app.py
```
