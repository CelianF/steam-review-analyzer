# Steam Review Analyzer (Jev demo)

Pick a Steam game: the app fetches its recent English reviews, and Jev judges each one
(main topic, real sentiment from 0 to 4, sarcasm, constructive or not). The dashboard
fills in live.

## Run

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
echo 'JEV_API_KEY=your_key' > .env    # skip if the file already exists
.venv/bin/uvicorn app:app --port 8000
```

Then open http://localhost:8000

## How it works

- `app.py`: FastAPI. `/api/search` looks up a game on Steam, `/api/analyze` fetches its reviews,
  sends 4 typed questions to Jev per review (8 in parallel), and streams the results back (SSE).
- `static/index.html`: the dashboard (Chart.js).
- The questions sent to Jev are in `QUESTIONS` at the top of `app.py`. Edit them to change
  what gets analyzed.
