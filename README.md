# Mixpanel Payment Summary (Streamlit)

## What it does
1) Upload a Pipedrive deals CSV.
2) Build a canonical `email` column using this priority:
   - Person - Email - Other
   - Person - Email - Work
   - Person - Email - Home
   - email
3) Validates email fields:
   - If a preferred email column has a non-empty value but it is not a valid email, the app logs the row number.
   - If no email is found after coalescing, the app logs the row number.
4) Fetch Mixpanel events (one at a time):
   - New Payment Made
   - Refund Granted
5) Outputs:
   - Summary by email (Total, Refund, Net)
   - Deals left-joined with those columns
   - Breakdown by Deal - Owner

## Local run (beginner steps)
1) Install Python 3.10+.
2) Open terminal inside this folder.
3) Create and activate virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate
```

4) Install dependencies:
```bash
pip install -r requirements.txt
```

5) Create secrets (never commit):
- Copy `.streamlit/secrets.toml.example` to `.streamlit/secrets.toml`
- Fill real values

6) Run:
```bash
streamlit run app.py
```

## Publish to GitHub (public safe)
- `.streamlit/secrets.toml` is gitignored.
- CSV and XLSX files are gitignored.

## Deploy to Streamlit Community Cloud
- Deploy this repo.
- Add secrets in the Streamlit Cloud app settings (Secrets panel).
