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

### LinkedIn Outreach (`/outreach`)
| Var | Purpose | Default |
|-----|---------|---------|
| `AGENCY_NAME` | Agency name in outreach copy | `HipHype Tech` |
| `SENDER_NAME` | Sender / sign-off name | `Ashish` |
| `LINKEDIN_FOLLOWUP_DAYS` | Count + spacing of auto follow-ups | `1,1,1` |
| `QUALITY_FILTER` | `off` disables the lead quality gate | `on` |
| `MIN_FIT` | Min lead `fit_score` to queue for outreach | `6` |
| `STALE_INVITE_DAYS` | Flag unaccepted invites older than this | `30` |
| `NOTE_MODEL` | GLM model for note/messages/auto follow-ups | `glm-4.5-air` |
| `REPLY_MODEL` | GLM model for the 3 "Interested" follow-ups | `glm-4.6` |

The **`/outreach`** page is a manual, AI-assisted worklist: it pulls qualified leads, writes the
LinkedIn copy for each stage (connection note → first message → follow-ups), tracks each prospect
through a 7-stage pipeline, and tells you exactly what to copy-paste into LinkedIn. **Nothing is
auto-sent** — you send every message by hand (there is no official LinkedIn send API). The daily 9 AM
run auto-queues newly found leads; you can also click "Sync from leads" anytime.

## Run locally
```
pip install -r requirements.txt        # only needed if using Postgres
export DATA_API_KEY=... Z_AI_API_KEY=... CONTACTOUT_EMAIL_KEY=... CONTACTOUT_PHONE_KEY=...
python app.py            # -> http://localhost:8000
```

## Daily run (built-in scheduler)
The web app has a **built-in daily scheduler** — no external cron needed. On startup it spawns a
background thread that runs the full pipeline (discovery -> qualify -> personalize -> store) once
per day and writes the results to the DB, so fresh leads are simply there each morning.

| Var | Purpose | Default |
|-----|---------|---------|
| `CRON_AT_UTC` | Time of the daily run, `HH:MM` in **UTC** | `03:30` (= 09:00 IST) |
| `CRON_ENABLED` | Set to `0` to disable the scheduler | `1` (on) |

To change the time, set `CRON_AT_UTC` (e.g. `14:00` for 2 PM UTC). It survives restarts/redeploys —
the thread just recomputes the next run.

You can also trigger a one-off run from the CLI (e.g. an external cron, if you prefer):
```
python app.py --cron     # runs the pipeline once and exits
```

## Deploy (Railway)
1. Connect this repo to a Railway project.
2. Add a **Postgres** plugin (provides `DATABASE_URL`).
3. Set the env vars above in the service.
4. Add a second **cron service** from the same repo: start command `python app.py --cron`, schedule `0 9 * * *`.
