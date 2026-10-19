"""Steam review analyzer: fetches a game's recent Steam reviews and has Jev judge each one."""

import asyncio
import json
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse

load_dotenv()
JEV_API_KEY = os.environ.get("JEV_API_KEY")
JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-latest"
JEV_PRICE_PER_M_INPUT = 0.042  # USD per 1M input tokens; output tokens are free
CONCURRENCY = 16
MAX_REVIEWS = 5000
MAX_REVIEW_CHARS = 3000
MIN_REVIEW_CHARS = 15

TOPICS = {
    "bugs": "crashes, glitches, broken features, technical problems",
    "performance": "FPS, optimization, lag, loading times, hardware requirements",
    "price": "cost, value for money, DLC, microtransactions, sales",
    "gameplay": "mechanics, controls, difficulty, how fun it is to play",
    "story": "plot, characters, writing, quests, atmosphere",
    "content": "amount of content, length, replayability, updates",
    "multiplayer": "online play, servers, matchmaking, community, cheaters",
    "other": "anything else, or no clear topic (memes, one-word reviews)",
}
SENTIMENT_LEVELS = ["very negative", "negative", "neutral", "positive", "very positive"]

QUESTIONS = {
    "topic": {
        "type": "choice",
        "instructions": "What is the main topic this Steam review talks about?",
        "criteria": TOPICS,
    },
    "sentiment": {
        "type": "score",
        "instructions": "What is the writer's REAL feeling toward the game? Read through sarcasm and irony.",
        "criteria": SENTIMENT_LEVELS,
    },
    "sarcasm": {
        "type": "noul",
        "instructions": "Is this review sarcastic or ironic (saying the opposite of what the writer means)?",
    },
    "constructive": {
        "type": "noul",
        "instructions": "Does this review give specific, useful feedback a game developer could act on?",
    },
}

app = FastAPI()
STATIC = Path(__file__).parent / "static"


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/search")
async def search(q: str = Query(min_length=1)):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(
            "https://store.steampowered.com/api/storesearch/",
            params={"term": q, "cc": "us", "l": "en"},
        )
        r.raise_for_status()
    return [
        {"id": it["id"], "name": it["name"], "image": it.get("tiny_image")}
        for it in r.json().get("items", [])
        if it.get("type") == "app"
    ][:8]


async def fetch_review_pages(client: httpx.AsyncClient, appid: int, n: int):
    """Yields (new_reviews, summary) one Steam page (100 reviews) at a time, until n usable reviews."""
    cursor, seen, summary = "*", set(), None
    while len(seen) < n:
        r = await client.get(
            f"https://store.steampowered.com/appreviews/{appid}",
            params={
                "json": 1, "filter": "recent", "language": "english",
                "purchase_type": "all", "num_per_page": 100, "cursor": cursor,
            },
        )
        r.raise_for_status()
        data = r.json()
        summary = summary or data.get("query_summary")
        batch = data.get("reviews", [])
        if not batch or data.get("cursor") == cursor:
            break
        cursor = data["cursor"]
        page = []
        for rev in batch:
            text = rev["review"].strip()
            # Steam's cursor sometimes repeats reviews across pages, hence the id check
            if len(text) < MIN_REVIEW_CHARS or rev["recommendationid"] in seen or len(seen) >= n:
                continue
            seen.add(rev["recommendationid"])
            page.append({
                "id": rev["recommendationid"],
                "text": text[:MAX_REVIEW_CHARS],
                "voted_up": rev["voted_up"],
                "timestamp": rev["timestamp_created"],
                "playtime_h": round(rev["author"].get("playtime_at_review", 0) / 60, 1),
            })
        yield page, summary


async def ask_jev(client: httpx.AsyncClient, review: dict):
    body = {"model": JEV_MODEL, "state": review["text"], "questions": QUESTIONS}
    for attempt in range(5):
        r = await client.post(JEV_URL, json=body, headers={"Authorization": f"Bearer {JEV_API_KEY}"})
        if r.status_code in (429, 529):
            await asyncio.sleep(0.5 * 2**attempt)
            continue
        r.raise_for_status()
        data = r.json()
        a = data["answers"]
        return {
            **review,
            "topic": a["topic"]["choice"],
            "sentiment": a["sentiment"]["score"] + 1,  # Jev levels are 0..4, shown as 1..5
            "sarcasm": a["sarcasm"]["noul"],
            "constructive": a["constructive"]["noul"],
            "tokens": data["usage"]["input_tokens"],
        }
    raise RuntimeError("Jev is rate limiting, try again in a moment")


def sse(event: str, data) -> str:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


@app.get("/api/analyze")
async def analyze(appid: int, n: int = Query(default=200, ge=10, le=MAX_REVIEWS)):
    if not JEV_API_KEY:
        raise HTTPException(500, "JEV_API_KEY missing from .env")

    async def stream():
        events: asyncio.Queue = asyncio.Queue()
        sem = asyncio.Semaphore(CONCURRENCY)
        workers: list[asyncio.Task] = []

        async def worker(rev):
            async with sem:
                try:
                    await events.put(("review", await ask_jev(client, rev)))
                except Exception as e:  # one bad review shouldn't kill the demo
                    await events.put(("error", str(e)))

        async def producer():
            # Analysis starts on the first page while later pages are still being fetched
            fetched = 0
            try:
                async for page, summary in fetch_review_pages(client, appid, n):
                    fetched += len(page)
                    workers.extend(asyncio.create_task(worker(r)) for r in page)
                    await events.put(("progress", {"fetched": fetched, "summary": summary,
                                                   "price_per_m": JEV_PRICE_PER_M_INPUT}))
            except httpx.HTTPError as e:
                await events.put(("error", f"Steam error: {e}"))
            await events.put(("fetch_done", {"total": fetched}))
            await asyncio.gather(*workers)
            await events.put(("done", {}))

        async with httpx.AsyncClient(timeout=30) as client:
            task = asyncio.create_task(producer())
            try:
                while True:
                    event, data = await events.get()
                    yield sse(event, data)
                    if event == "done":
                        break
            finally:
                # Browser closed or new analysis started: stop spending Jev credits
                task.cancel()
                for w in workers:
                    w.cancel()

    return StreamingResponse(stream(), media_type="text/event-stream")
