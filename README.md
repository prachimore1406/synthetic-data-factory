# Synthetic Data Factory

Agentic synthetic data factory for co-designing schemas and generating deterministic datasets.

- UI: Streamlit (Chat → Human Review → Run)
- API: FastAPI
- Design: CrewAI generates CMC/RPC contracts
- Build: LangGraph orchestrates DDL, generation, optional Postgres insert, CSV export, QA

## Quick start

1. Create a local environment file:

   - Copy `.env.example` to `.env`
   - Set `OPENAI_API_KEY` and Postgres connection values

2. Install deps:

   ```bash
   pip install -r app/requirements.txt
   ```

3. Start API + UI:

   ```bash
   uvicorn app.api.main:app --reload --port 8000
   streamlit run app/ui/streamlit_app.py
   ```

## Outputs

- Run artifacts: `artifacts/runs/<run_id>/`
- CSV export: `artifacts/runs/<run_id>/csv/<table>.csv`
- Optional Postgres insert: `POST /run` with `sink="postgres"` (default) or `sink="csv"`

## Docs

- Detailed architecture and usage: [docs/README_Synthetic_Data_Factory_v1.md](docs/README_Synthetic_Data_Factory_v1.md)
- Design thinking notes: [docs/DOUBLE_DIAMOND_SYNTHETIC_DATA_FACTORY.md](docs/DOUBLE_DIAMOND_SYNTHETIC_DATA_FACTORY.md)

