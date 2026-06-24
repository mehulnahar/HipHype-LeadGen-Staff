# HipHype Lead Finder

Agentic lead-generation tool for IT staff augmentation. Discovers funded, right-sized, hiring software companies; an AI agent qualifies them, another drafts a personalized outreach opener; surfaces the decision-makers to connect with; enriches email/phone on demand.

## Stack
- **Backend:** zero-framework Python (`app.py`, stdlib `http.server`) — only external dep is `psycopg2-binary` (for Postgres).
- **Data API:** Coresignal (company discovery + firmographics).
- **AI agents:** Z.ai GLM (qualifier, personalizer, "what they do" summary).
- **Contacts:** ContactOut (email + phone), on demand.
- **Storage:** Postgres when `DATABASE_URL` is set; local JSON files otherwise.

## Environment variables
| Var | Purpose |
|-----|---------|
| `DATA_API_KEY` | Coresignal API key |
| `Z_AI_API_KEY` | Z.ai (GLM) API key |
| `CONTACTOUT_EMAIL_KEY` | ContactOut email enrichment |
| `CONTACTOUT_PHONE_KEY` | ContactOut phone enrichment |
| `DATABASE_URL` | Postgres connection string (auto-set by Railway) |
| `PORT` | HTTP port (auto-set by Railway; default 8000) |

## Run locally
```
pip install -r requirements.txt        # only needed if using Postgres
export DATA_API_KEY=... Z_AI_API_KEY=... CONTACTOUT_EMAIL_KEY=... CONTACTOUT_PHONE_KEY=...
python app.py            # -> http://localhost:8000
```

## Daily run (cron)
```
python app.py --cron     # discovery -> qualify -> personalize -> store
```
On a server, schedule it for 9 AM:
```
0 9 * * * cd /path/to/app && python3 app.py --cron >> cron.log 2>&1
```

## Deploy (Railway)
1. Connect this repo to a Railway project.
2. Add a **Postgres** plugin (provides `DATABASE_URL`).
3. Set the env vars above in the service.
4. Add a second **cron service** from the same repo: start command `python app.py --cron`, schedule `0 9 * * *`.
