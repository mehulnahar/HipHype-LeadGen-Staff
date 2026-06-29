#!/usr/bin/env python3
"""
HipHype Lead Finder — zero-dependency web app.

Run:
    set DATA_API_KEY=your_key      (Windows)   /   export DATA_API_KEY=...  (mac/linux)
    python app.py
    open http://localhost:8000

Backend holds the API key (never exposed to the browser) and runs the
discovery -> qualify -> buyer -> job/JD pipeline.
"""
import os, re, json, ssl, urllib.request, urllib.error, urllib.parse
import threading, time, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

API_KEY = os.environ.get("DATA_API_KEY", "")
BASE = "https://api.coresignal.com/cdapi/v2"
CTX = ssl.create_default_context(); CTX.check_hostname = False; CTX.verify_mode = ssl.CERT_NONE
HDR = {"apikey": API_KEY, "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}

class CreditError(Exception):
    pass

def _raise_402(e):
    if isinstance(e, urllib.error.HTTPError) and e.code == 402:
        raise CreditError("Data provider: insufficient credits - please top up your account to continue.")

def _post(path, body):
    r = urllib.request.Request(BASE + path, data=json.dumps(body).encode(), headers=HDR, method="POST")
    try:
        return json.loads(urllib.request.urlopen(r, timeout=60, context=CTX).read())
    except urllib.error.HTTPError as e:
        _raise_402(e); raise

def _get(path):
    r = urllib.request.Request(BASE + path, headers=HDR)
    try:
        return json.loads(urllib.request.urlopen(r, timeout=60, context=CTX).read())
    except urllib.error.HTTPError as e:
        _raise_402(e); raise

def _clean(t): return re.sub(r"\s+", " ", (t or "")).strip()

MONTHS = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
def _fmt_amount(amt, cur=None):
    if not isinstance(amt, (int, float)) or not amt:
        return ""
    a = float(amt)
    if a >= 1e9:   s = f"${a/1e9:.1f}B"
    elif a >= 1e6: s = f"${a/1e6:.1f}M"
    elif a >= 1e3: s = f"${a/1e3:.0f}K"
    else:          s = f"${a:.0f}"
    return s.replace(".0B", "B").replace(".0M", "M")
def _fmt_date(d):
    if not d or len(d) < 7:
        return d or ""
    try:
        return f"{MONTHS[int(d[5:7])]} {d[:4]}"
    except Exception:
        return d

ZAI_KEY = os.environ.get("Z_AI_API_KEY", "")
ZAI_URL = "https://api.z.ai/api/paas/v4/chat/completions"
def _zai_chat(prompt, max_tokens=130, temp=0.2, model="glm-4.5-air"):
    """One Z.ai GLM chat call -> plain text content (or '' on failure)."""
    if not ZAI_KEY:
        return ""
    body = {"model": model, "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens, "temperature": temp, "thinking": {"type": "disabled"}}
    try:
        r = urllib.request.Request(ZAI_URL, data=json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + ZAI_KEY, "Content-Type": "application/json",
                     "User-Agent": "Mozilla/5.0"}, method="POST")
        d = json.loads(urllib.request.urlopen(r, timeout=45, context=CTX).read())
        return _clean(d["choices"][0]["message"]["content"])
    except Exception:
        return ""

def _zai_summarize(name, desc):
    """What they do, in 2-3 plain lines."""
    if not desc:
        return ""
    return _zai_chat("In 2-3 short, plain lines, describe what this company does (its product/service and who "
                     "it's for). No marketing adjectives, no hype.\n\nCompany: %s\nInfo: %s" % (name, desc[:1600]), 130, 0.2)

def _qualify_agent(name, desc, role, hq, employees, funding):
    """Z.ai GLM judge: is this a real staff-aug prospect? -> {verdict, score, reason}."""
    if not ZAI_KEY:
        return {"verdict": "keep", "score": 0, "reason": ""}
    prompt = (
        "You qualify leads for an IT staff-augmentation firm (offshore engineers in India placed with global clients). "
        "GOOD = a real product/software company that builds software and could need extra engineering capacity. "
        "DROP = staffing agencies, dev shops / IT-services firms (competitors), recruiters, AI data-labeling or gig "
        "platforms, or non-software companies. "
        "Reply with ONLY compact JSON: {\"verdict\":\"keep\" or \"drop\",\"score\":1-10,\"reason\":\"<=12 words\"}\n\n"
        "Company: %s\nHQ: %s | Employees: %s | Funding: %s\nHiring: %s\nAbout: %s"
        % (name, hq, employees, funding, role, (desc or "")[:900])
    )
    txt = _zai_chat(prompt, 120, 0.0)
    try:
        m = re.search(r"\{.*\}", txt, re.S)
        j = json.loads(m.group(0)) if m else {}
        return {"verdict": str(j.get("verdict") or "keep").lower(),
                "score": int(j.get("score") or 0),
                "reason": str(j.get("reason") or "")}
    except Exception:
        return {"verdict": "keep", "score": 0, "reason": ""}

def _personalize_agent(company, product, role, buyer, funding):
    """Z.ai GLM: a short LinkedIn outreach opener for this lead."""
    if not ZAI_KEY:
        return ""
    prompt = (
        "Write a SHORT LinkedIn outreach opener (2-3 sentences, ~45 words) from an IT staff-augmentation provider to "
        "the person below. Be human and specific; lead with their context, not a hard pitch. Reference what the company "
        "does and that they're hiring; offer pre-vetted remote engineers to add capacity fast. "
        "No 'Dear', no hashtags, no emojis.\n\n"
        "To: %s at %s\nWhat %s does: %s\nThey're hiring: %s\nRecent funding: %s"
        % (buyer or "the team", company, company, product or "", role or "engineers", funding or "")
    )
    return _zai_chat(prompt, 150, 0.4)

# --- storage: Postgres when DATABASE_URL is set (Railway), else local JSON files ---
DATABASE_URL = os.environ.get("DATABASE_URL", "")
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SEEN_FILE = os.path.join(_BASE_DIR, "seen_companies.json")
LEADS_FILE = os.path.join(_BASE_DIR, "leads.json")
_DB = {"conn": None}

def _db():
    if not DATABASE_URL:
        return None
    try:
        import psycopg2
        c = _DB.get("conn")
        if c is None or getattr(c, "closed", 1):
            c = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            c.autocommit = True
            with c.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS leads (company TEXT PRIMARY KEY, data TEXT, updated_at TIMESTAMP DEFAULT now())")
                cur.execute("CREATE TABLE IF NOT EXISTS seen_companies (id BIGINT PRIMARY KEY)")
                cur.execute("CREATE TABLE IF NOT EXISTS outreach (id TEXT PRIMARY KEY, data TEXT, updated_at TIMESTAMP DEFAULT now())")
            _DB["conn"] = c
        return c
    except Exception as e:
        print("DB error:", e)
        return None

def _load_seen():
    c = _db()
    if c:
        try:
            with c.cursor() as cur:
                cur.execute("SELECT id FROM seen_companies")
                return set(r[0] for r in cur.fetchall())
        except Exception:
            return set()
    try:
        with open(SEEN_FILE) as f:
            return set(json.load(f))
    except Exception:
        return set()

def _save_seen(s):
    c = _db()
    if c:
        try:
            rows = [(int(x),) for x in s if str(x).isdigit()]
            with c.cursor() as cur:
                cur.executemany("INSERT INTO seen_companies (id) VALUES (%s) ON CONFLICT DO NOTHING", rows)
        except Exception:
            pass
        return
    try:
        with open(SEEN_FILE, "w") as f:
            json.dump(sorted(s), f)
    except Exception:
        pass

def _load_leads():
    c = _db()
    if c:
        try:
            with c.cursor() as cur:
                cur.execute("SELECT data FROM leads ORDER BY updated_at DESC LIMIT 1000")
                return [json.loads(r[0]) for r in cur.fetchall()]
        except Exception:
            return []
    try:
        with open(LEADS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_leads(leads):
    c = _db()
    if c:
        try:
            with c.cursor() as cur:
                for l in leads:
                    cur.execute("INSERT INTO leads (company, data, updated_at) VALUES (%s,%s,now()) "
                                "ON CONFLICT (company) DO UPDATE SET data=EXCLUDED.data, updated_at=now()",
                                (l.get("company") or "", json.dumps(l)))
        except Exception as e:
            print("save_leads error:", e)
        return
    try:
        with open(LEADS_FILE, "w", encoding="utf-8") as f:
            json.dump(leads, f)
    except Exception:
        pass

def _persist(new_leads):
    """Upsert new leads (dedup by company). Postgres upserts; file mode merges + caps 500."""
    if _db():
        _save_leads(new_leads)
        return _load_leads()
    names = {l.get("company") for l in new_leads}
    merged = list(new_leads) + [l for l in _load_leads() if l.get("company") not in names]
    _save_leads(merged[:500])
    return merged

# --- ContactOut: on-demand email/phone enrichment (only for leads you pick) ---
CO_EMAIL_KEY = os.environ.get("CONTACTOUT_EMAIL_KEY", "")
CO_PHONE_KEY = os.environ.get("CONTACTOUT_PHONE_KEY", "")
CO_BASE = "https://api.contactout.com"

def _co_call(token, name, company, linkedin, include):
    if linkedin:
        body = {"linkedin_url": linkedin, "include": include}
    else:
        body = {"full_name": name, "company": ([company] if company else []), "include": include}
    r = urllib.request.Request(CO_BASE + "/v1/people/enrich", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "token": token, "User-Agent": "Mozilla/5.0"}, method="POST")
    return json.loads(urllib.request.urlopen(r, timeout=45, context=CTX).read()).get("profile", {})

def _co_enrich(name, company, linkedin=""):
    out = {"email": "", "phone": "", "linkedin": linkedin or ""}
    if CO_EMAIL_KEY:
        try:
            p = _co_call(CO_EMAIL_KEY, name, company, linkedin, ["work_email", "personal_email"])
            emails = p.get("work_email") or p.get("email") or p.get("personal_email") or []
            if emails: out["email"] = emails[0]
            if not out["linkedin"]: out["linkedin"] = p.get("url") or p.get("linkedin_url") or ""
        except Exception:
            pass
    if CO_PHONE_KEY:
        try:
            p = _co_call(CO_PHONE_KEY, name, company, linkedin, ["phone"])
            phones = p.get("phone") or []
            if phones: out["phone"] = phones[0]
            if not out["linkedin"]: out["linkedin"] = p.get("url") or p.get("linkedin_url") or ""
        except Exception:
            pass
    return out

# --- QA: buyer selection (prefer eng decision-maker, reject finance/sales/etc.) ---
EXEC_PRI = ["chief technology officer", "cto", "vp of engineering", "vp engineering",
            "head of engineering", "engineering", "co-founder & ceo", "chief executive",
            "ceo", "co-founder", "founder"]
BAD_EXEC = ["finance", "sales", "account", "marketing", "people", "talent", "hr ",
            "human resources", "customer", "operations", "production",
            "board", "investor", "advisor", "observer", "venture", "partner"]
ENG_KW   = ["engineer", "developer", "software", "frontend", "front-end", "backend", "back-end",
            "full stack", "fullstack", "mobile", "react", "devops", "ml", "ai", "data", "platform"]

def _pick_buyer(execs):
    if not execs: return {}
    for p in EXEC_PRI:
        for e in execs:
            t = (e.get("member_position_title") or "").lower()
            if p in t and not any(b in t for b in BAD_EXEC):
                return e
    for e in execs:
        t = (e.get("member_position_title") or "").lower()
        if not any(b in t for b in BAD_EXEC):
            return e
    return execs[0]

def _rank_execs(execs, limit=3):
    """Top decision-makers to connect with: eng/exec leaders, excluding finance/sales/etc."""
    ranked = []
    for p in EXEC_PRI:
        for e in execs:
            t = (e.get("member_position_title") or "").lower()
            if p in t and not any(b in t for b in BAD_EXEC) and e not in ranked:
                ranked.append(e)
    for e in execs:
        t = (e.get("member_position_title") or "").lower()
        if not any(b in t for b in BAD_EXEC) and e not in ranked:
            ranked.append(e)
    return ranked[:limit]

def find_leads(cfg):
    size_min    = int(cfg.get("size_min", 20))
    size_max    = int(cfg.get("size_max", 200))
    funded      = cfg.get("funded_since", "2026-04-01")
    industry    = cfg.get("industry", "Software Development")
    country     = (cfg.get("country") or "").strip()
    max_results = min(int(cfg.get("max_results", 10)), 25)

    filters = [
        {"range": {"employees_count": {"gte": size_min, "lte": size_max}}},
        {"range": {"active_job_postings_count": {"gte": 3}}},
        {"range": {"last_funding_round.announced_date": {"gte": funded}}},
        {"match": {"industry": industry}},
    ]
    if country:
        filters.append({"match": {"hq_country": country}})
    ids = _post("/company_multi_source/search/es_dsl", {"query": {"bool": {"filter": filters}}})
    if not isinstance(ids, list):
        return []

    seen = _load_seen()
    leads = []
    for cid in ids:
        if len(leads) >= max_results:
            break
        if cid in seen:
            continue
        try:
            d = _get("/company_multi_source/collect/" + str(cid))   # the ONLY Coresignal collect per lead
        except CreditError:
            raise
        except Exception:
            continue
        seen.add(cid)
        lf = d.get("last_funding_round") or {}
        comp = d.get("company_name") or ""
        # decision-makers: names + titles are FREE in the company record (NO employee collect)
        dms = []
        for ex in _rank_execs(d.get("key_executives") or [], 3):
            nm = ex.get("member_full_name") or ""
            dms.append({
                "name": nm,
                "title": ex.get("member_position_title") or "",
                "company": comp,
                "linkedin": "",
                "linkedin_search": "https://www.google.com/search?q=" + urllib.parse.quote((nm + " " + comp + " site:linkedin.com/in").strip()),
                "email": "",
                "phone": "",
            })
        b0 = dms[0] if dms else {}
        product = _zai_summarize(comp, d.get("description_enriched") or d.get("description"))
        # job: an eng-relevant active posting; link CONSTRUCTED from id (NO job collect)
        job = {}
        posts = d.get("active_job_postings") or []
        for j in posts:
            if any(k in (j.get("job_posting_title") or "").lower() for k in ENG_KW):
                job = j; break
        if not job and posts:
            job = posts[0]
        jid = job.get("job_posting_id")
        # hiring momentum (free, already in the company record) — open roles + monthly change
        cc = d.get("active_job_postings_count_change") or {}
        jobs_open = d.get("active_job_postings_count") or cc.get("current") or 0
        jobs_change_m = cc.get("change_monthly")
        amt = lf.get("amount_raised")
        funding = " ".join(str(x) for x in [lf.get("type"), lf.get("announced_date"),
                  (f"${amt:,}" if isinstance(amt, int) else ""), lf.get("amount_raised_currency") or ""] if x).strip()
        # AGENT 1 - qualify (Z.ai judge): drop competitors/agencies/non-fits
        q = _qualify_agent(comp, d.get("description_enriched") or d.get("description") or "",
                           job.get("job_posting_title") or "", d.get("hq_country"), d.get("employees_count"), funding)
        if q.get("verdict") == "drop":
            continue
        # AGENT 2 - personalize (Z.ai): outreach opener for this lead
        opener = _personalize_agent(comp, product, job.get("job_posting_title") or "", b0.get("name", ""), funding)
        leads.append({
            "company": comp,
            "fit_score": q.get("score", 0),
            "fit_reason": q.get("reason", ""),
            "opener": opener,
            "product": product,
            "hq": d.get("hq_country"),
            "employees": d.get("employees_count"),
            "funding": funding,
            "funding_round": lf.get("type") or "",
            "funding_amount": _fmt_amount(amt),
            "funding_date": _fmt_date(lf.get("announced_date")),
            "job_title": job.get("job_posting_title") or "",
            "job_link": ("https://www.linkedin.com/jobs/view/" + str(jid)) if jid else "",
            "job_active": 1,
            "jobs_open": jobs_open,
            "jobs_change_m": jobs_change_m,
            "jd": "",
            "buyer": b0.get("name", ""),
            "buyer_title": b0.get("title", ""),
            "decision_makers": dms,
            "linkedin": "",
            "email": "",
            "phone": "",
            "website": d.get("website") or "",
        })
    _save_seen(seen)
    return leads

# ============================ LinkedIn Outreach (manual, AI-assisted) ============================
# A worklist that picks good leads, writes the copy for each stage, tracks pipeline position, and tells
# the operator exactly what to paste into LinkedIn. Nothing is auto-sent — one human sends by hand.
import time as _time

AGENCY        = os.environ.get("AGENCY_NAME", "HipHype Tech")
SENDER        = os.environ.get("SENDER_NAME", "Ashish")
FOLLOWUP_DAYS = [int(x) for x in os.environ.get("LINKEDIN_FOLLOWUP_DAYS", "1,1,1").split(",") if x.strip().isdigit()] or [1, 1, 1]
MAX_FOLLOWUPS = len(FOLLOWUP_DAYS)
STALE_INVITE_DAYS = int(os.environ.get("STALE_INVITE_DAYS", "30"))
QUALITY_FILTER = os.environ.get("QUALITY_FILTER", "on").lower() != "off"
MIN_FIT       = int(os.environ.get("MIN_FIT", "6"))
NOTE_MODEL    = os.environ.get("NOTE_MODEL", "glm-4.5-air")
REPLY_MODEL   = os.environ.get("REPLY_MODEL", "glm-4.6")
DAY = 86400

def _strip_dashes(s):
    return re.sub(r", ,", ",", re.sub(r"\s*[—–]\s*", ", ", str(s or ""))).strip()

OUTREACH_FILE = os.path.join(_BASE_DIR, "outreach.json")

def _load_outreach():
    c = _db()
    if c:
        try:
            with c.cursor() as cur:
                cur.execute("SELECT data FROM outreach ORDER BY updated_at DESC")
                return [json.loads(r[0]) for r in cur.fetchall()]
        except Exception:
            return []
    try:
        with open(OUTREACH_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_outreach_row(row):
    row["updated"] = _time.time()
    c = _db()
    if c:
        try:
            with c.cursor() as cur:
                cur.execute("INSERT INTO outreach (id, data, updated_at) VALUES (%s,%s,now()) "
                            "ON CONFLICT (id) DO UPDATE SET data=EXCLUDED.data, updated_at=now()",
                            (row.get("company") or "", json.dumps(row)))
        except Exception as e:
            print("save_outreach error:", e)
        return
    rows = [r for r in _load_outreach() if r.get("company") != row.get("company")]
    rows.append(row)
    try:
        with open(OUTREACH_FILE, "w", encoding="utf-8") as f:
            json.dump(rows, f)
    except Exception:
        pass

def _get_outreach(company):
    for r in _load_outreach():
        if r.get("company") == company:
            return r
    return None

def _first_name(name):
    n = (name or "").strip()
    return n.split(" ")[0] if n else ""

def _pick_dm(lead):
    """Decision-maker to contact: prefer one with a real /in/ LinkedIn, else the first."""
    dms = lead.get("decision_makers") or []
    for dm in dms:
        if "/in/" in (dm.get("linkedin") or ""):
            return dm
    return dms[0] if dms else {}

def _passes_gate(lead, dm):
    if not _first_name(dm.get("name")):          # name guard: never "Hi there"
        return False
    if not QUALITY_FILTER:
        return True
    return int(lead.get("fit_score") or 0) >= MIN_FIT

def _outreach_intake():
    """Promote qualifying leads into 'queued' rows. Never overwrites existing rows (preserves progress)."""
    existing = {r.get("company") for r in _load_outreach()}
    created = 0
    for lead in _load_leads():
        comp = lead.get("company")
        if not comp or comp in existing:
            continue
        dm = _pick_dm(lead)
        if not _passes_gate(lead, dm):
            continue
        _save_outreach_row({
            "company": comp, "first_name": _first_name(dm.get("name")),
            "dm_name": dm.get("name") or "", "dm_title": dm.get("title") or "",
            "linkedin_url": dm.get("linkedin") if "/in/" in (dm.get("linkedin") or "") else "",
            "linkedin_search": dm.get("linkedin_search") or "",
            "job_title": lead.get("job_title") or "", "product": lead.get("product") or "",
            "fit_score": lead.get("fit_score") or 0, "status": "queued",
            "connection_note": "", "messages": [], "connection_sent_at": None, "connected_at": None,
            "followup_due_at": None, "followup_count": 0, "interested": False, "interested_at": None,
            "last_reply_text": "", "scheduled": False, "scheduled_messages": [], "closed_at": None,
            "created_at": _time.time(),
        })
        existing.add(comp); created += 1
    return created

# --- AI copy (Z.ai GLM); every output passes through the dash-stripper ---
SHARED_RULES = (
    "Style: warm, curious, genuinely human, like a real person who read about them, NOT a vendor pitching. "
    "Do NOT: make performance claims or use numbers/percentages; use buzzwords (essence, elevate, unlock, captivate, "
    "drive, stunning, seamless, leverage, supercharge, game-changing); write 'we specialize in'; give prescriptive "
    "'you should do X'; use the word 'available'; offer to send a Loom, case study, portfolio, samples or any attachment. "
    "Do NOT use em dashes or en dashes; use commas or periods. CTA: gently propose a quick call at 11 AM their time "
    "tomorrow, and clearly leave it to them (\"no pressure if that doesn't suit\")."
)

def _ctx(row):
    return {"FN": row.get("first_name") or "there", "ROLE": row.get("dm_title") or "",
            "JOB": row.get("job_title") or "", "DESC": (row.get("product") or "")[:800], "COMP": row.get("company") or ""}

def _gen_note(row):
    c = _ctx(row); role = (", " + c["ROLE"]) if c["ROLE"] else ""
    prompt = (
        "You are writing a short LinkedIn connection-request note (the 'add a note' field, sent WITH the invite).\n\n"
        "Sender: %s from %s (IT staff augmentation, offshore engineers placed with global clients)\n"
        "Recipient: %s%s at %s\nWhat their company does: %s\nThey are hiring: %s\n\n"
        "Write the note in EXACTLY this structure:\n\n"
        "<TOPIC LINE: a concise plain-language summary of what their company does or is hiring for. Max 60 characters. No 'Subject' label.>\n\n"
        "Hi %s,\n\n<one short friendly genuine line: introduce yourself by name and company, reference ONE specific detail about "
        "their company/role that shows you actually looked, and that you can help add engineering capacity.>\n\nThanks,\n%s\n\n"
        "Strict rules:\n1. The ENTIRE note MUST be under 300 characters total.\n2. Warm and human, not salesy, not formal. No 'Dear'.\n"
        "3. No performance claims or numbers. No buzzwords. Never the word 'available'.\n4. No em dashes or en dashes; commas or periods.\n"
        "5. Return ONLY the note text in the structure above. No commentary, no character count."
        % (SENDER, AGENCY, c["FN"], role, c["COMP"], c["DESC"], c["JOB"] or "engineers", c["FN"], SENDER)
    )
    return _strip_dashes(_zai_chat(prompt, 240, 0.5, model=NOTE_MODEL))

def _gen_first(row):
    c = _ctx(row); role = (", " + c["ROLE"]) if c["ROLE"] else ""
    prompt = (
        "%s just accepted %s's LinkedIn connection. Write the FIRST message to send now.\n\n"
        "Sender: %s from %s\nRecipient: %s%s\nTheir company does: %s\nThey are hiring: %s\n"
        "Connection note already sent (do NOT repeat it): %s\n\n"
        "Lead with sincere interest in THEIR work and goal. Add at most one modest genuine thought or a thoughtful question, "
        "then the soft CTA. Brief and conversational, like a real person reaching out.\n\n%s\n\n"
        "Rules:\n1. 350-470 characters.\n2. Sign off as %s.\n3. Return ONLY the message text. No commentary."
        % (c["FN"], SENDER, SENDER, AGENCY, c["FN"], role, c["DESC"], c["JOB"] or "engineers",
           row.get("connection_note") or "(none)", SHARED_RULES, SENDER)
    )
    return _strip_dashes(_zai_chat(prompt, 360, 0.5, model=NOTE_MODEL))

def _gen_followup(row, n):
    c = _ctx(row)
    prior = "\n".join("- " + (m.get("text") or "") for m in (row.get("messages") or []))
    prompt = (
        "%s has not replied yet. Write follow-up #%d (a light, friendly nudge).\n\n"
        "Sender: %s from %s\nRecipient: %s\nTheir company does: %s\nThey are hiring: %s\n"
        "Messages already sent (take a genuinely DIFFERENT angle, do NOT repeat these):\n%s\n\n"
        "Open with a light genuine thought or a curious question about their work from a NEW angle, then the soft CTA.\n\n%s\n\n"
        "Rules:\n1. 220-360 characters.\n2. Sign off as %s.\n3. Return ONLY the message text. No commentary."
        % (c["FN"], n, SENDER, AGENCY, c["FN"], c["DESC"], c["JOB"] or "engineers", prior or "(none)", SHARED_RULES, SENDER)
    )
    return _strip_dashes(_zai_chat(prompt, 320, 0.55, model=NOTE_MODEL))

def _gen_interested_followups(row):
    c = _ctx(row)
    prior = "\n".join("- " + (m.get("text") or "") for m in (row.get("messages") or []))
    reply = row.get("last_reply_text") or ""
    reply_line = ('Their latest reply: "%s"' % reply) if reply else "They engaged and were marked interested."
    prompt = (
        "%s is an interested prospect (%s is the sender, from %s).\nTheir company does: %s\nThey are hiring: %s\n%s\n"
        "Messages already sent:\n%s\n\n"
        "Write 3 warm, curious, NON-SALESY follow-up messages, sent on DIFFERENT days, that must NOT repeat each other. "
        'Return ONLY JSON: {"followups":["msg1","msg2","msg3"]}.\n\n'
        "Each: open with a light genuine thought or a curious question about THEIR work from a genuinely DIFFERENT angle, then the soft CTA.\n\n%s\n\n"
        "Hard rules:\n1. Each 220-360 chars, addressed to %s.\n2. Sign off as %s.\n3. It is fine that all three say 'tomorrow' (each is read on its own day)."
        % (c["FN"], SENDER, AGENCY, c["DESC"], c["JOB"] or "engineers", reply_line, prior or "(none)", SHARED_RULES, c["FN"], SENDER)
    )
    txt = _zai_chat(prompt, 800, 0.6, model=REPLY_MODEL)
    try:
        m = re.search(r"\{.*\}", txt, re.S)
        fus = json.loads(m.group(0)).get("followups", []) if m else []
    except Exception:
        fus = []
    return [_strip_dashes(x) for x in fus][:3]

def _schedule_first_followup(row):
    row["followup_count"] = 0
    row["followup_due_at"] = _time.time() + FOLLOWUP_DAYS[0] * DAY

def _advance_followup(row, text):
    msgs = row.get("messages") or []
    msgs.append({"seq": len(msgs), "kind": "followup", "text": text, "at": _time.time()})
    row["messages"] = msgs
    row["followup_count"] = int(row.get("followup_count") or 0) + 1
    row["followup_due_at"] = (_time.time() + FOLLOWUP_DAYS[row["followup_count"]] * DAY) if row["followup_count"] < MAX_FOLLOWUPS else None

def _due_now(row, now):
    if row.get("interested") and row.get("scheduled"):
        return any((not m.get("sent")) and (m.get("sendAt") or 0) <= now for m in (row.get("scheduled_messages") or []))
    if row.get("status") == "messaged" and not row.get("interested"):
        d = row.get("followup_due_at")
        return bool(d) and d <= now and int(row.get("followup_count") or 0) < MAX_FOLLOWUPS
    return False

def _has_pending(row):
    if row.get("interested") and row.get("scheduled"):
        return any(not m.get("sent") for m in (row.get("scheduled_messages") or []))
    if row.get("status") == "messaged" and not row.get("interested"):
        return bool(row.get("followup_due_at")) and int(row.get("followup_count") or 0) < MAX_FOLLOWUPS
    return False

def _in_tab(row, tab, now):
    st = row.get("status")
    closed = st in ("ignored", "stopped") or bool(row.get("closed_at"))
    if tab == "closed":     return closed
    if closed:              return False
    if tab == "to_send":    return st == "queued"
    if tab == "awaiting":   return st == "connection_sent"
    if tab == "connected":  return st in ("connected", "messaged") and not row.get("interested")
    if tab == "due":        return _due_now(row, now)
    if tab == "scheduled":  return _has_pending(row)
    if tab == "interested": return bool(row.get("interested"))
    return False

OUTREACH_TABS = ["to_send", "awaiting", "connected", "due", "scheduled", "interested", "closed"]

def _outreach_payload(tab="to_send", q=""):
    now = _time.time()
    allrows = _load_outreach()
    counts = {t: sum(1 for r in allrows if _in_tab(r, t, now)) for t in OUTREACH_TABS}
    rows = [r for r in allrows if _in_tab(r, tab, now)]
    if q:
        ql = q.lower()
        rows = [r for r in rows if ql in (r.get("company", "") + " " + r.get("dm_name", "") + " " +
                r.get("dm_title", "") + " " + r.get("job_title", "")).lower()]
    for r in rows:
        r["_invite_age_days"] = int((now - r["connection_sent_at"]) / DAY) if r.get("connection_sent_at") else None
        r["_stale"] = bool(r.get("_invite_age_days") is not None and r["_invite_age_days"] > STALE_INVITE_DAYS)
    if tab in ("due", "scheduled"):
        def _nd(r):
            if r.get("interested") and r.get("scheduled"):
                ds = [m.get("sendAt") or 0 for m in (r.get("scheduled_messages") or []) if not m.get("sent")]
                return min(ds) if ds else 9e18
            return r.get("followup_due_at") or 9e18
        rows.sort(key=_nd)
    else:
        rows.sort(key=lambda r: r.get("created_at") or 0, reverse=True)
    return {"tab": tab, "counts": counts, "rows": rows, "max_followups": MAX_FOLLOWUPS, "now": now}

def _outreach_action(company, action, payload):
    payload = payload or {}
    row = _get_outreach(company)
    if not row:
        return {"error": "lead not found"}
    now = _time.time()
    if action == "generate_note":
        row["connection_note"] = _gen_note(row); _save_outreach_row(row); return {"text": row["connection_note"]}
    if action == "generate_first":
        t = _gen_first(row); msgs = row.get("messages") or []
        msgs.append({"seq": len(msgs), "kind": "first", "text": t, "at": now}); row["messages"] = msgs
        _save_outreach_row(row); return {"text": t}
    if action == "generate_followup":
        return {"text": _gen_followup(row, int(row.get("followup_count") or 0) + 1)}
    if action == "generate_schedule":
        return {"drafts": _gen_interested_followups(row)}
    if action == "enrich":
        res = _co_enrich(row.get("dm_name", ""), row.get("company", ""), row.get("linkedin_url", ""))
        if res.get("linkedin"): row["linkedin_url"] = res["linkedin"]
        row["dm_email"] = res.get("email", ""); row["dm_phone"] = res.get("phone", "")
        _save_outreach_row(row)
        return {"linkedin": row.get("linkedin_url", ""), "email": res.get("email", ""), "phone": res.get("phone", "")}
    if action == "mark_sent":
        row["status"] = "connection_sent"; row["connection_sent_at"] = now
    elif action == "mark_connected":
        row["status"] = "connected"; row["connected_at"] = now
    elif action == "decline":
        row["status"] = "ignored"; row["closed_at"] = now
    elif action == "mark_messaged":
        txt = payload.get("text") or ""
        if txt and not any(m.get("kind") == "first" for m in (row.get("messages") or [])):
            msgs = row.get("messages") or []; msgs.append({"seq": len(msgs), "kind": "first", "text": txt, "at": now}); row["messages"] = msgs
        row["status"] = "messaged"; _schedule_first_followup(row)
    elif action == "mark_followup_sent":
        if row.get("interested") and row.get("scheduled"):
            pend = sorted([m for m in (row.get("scheduled_messages") or []) if not m.get("sent")], key=lambda m: m.get("sendAt") or 0)
            if pend: pend[0]["sent"] = True; pend[0]["sentAt"] = now
            if all(m.get("sent") for m in (row.get("scheduled_messages") or [])):
                row["status"] = "stopped"; row["closed_at"] = now
        else:
            _advance_followup(row, payload.get("text") or "")
            if row.get("followup_due_at") is None and int(row.get("followup_count") or 0) >= MAX_FOLLOWUPS:
                row["status"] = "stopped"; row["closed_at"] = now
    elif action == "replied":
        row["interested"] = True; row["interested_at"] = now; row["last_reply_text"] = payload.get("reply") or ""
        row["followup_due_at"] = None
        if row.get("closed_at"): row["closed_at"] = None; row["status"] = "messaged"
    elif action == "schedule":
        sm = []
        for i, m in enumerate(payload.get("messages") or []):
            sm.append({"seq": i, "text": m.get("text") or "", "sendAt": m.get("sendAt") or (now + (i + 1) * DAY), "sent": False})
        row["scheduled_messages"] = sm; row["scheduled"] = True
        if not row.get("interested"): row["interested"] = True; row["interested_at"] = now
    elif action == "close":
        row["status"] = "stopped"; row["closed_at"] = now
    elif action == "restore":
        row["closed_at"] = None
        row["status"] = ("messaged" if row.get("messages") or row.get("scheduled") else
                         "connected" if row.get("connected_at") else
                         "connection_sent" if row.get("connection_sent_at") else "queued")
    else:
        return {"error": "unknown action"}
    _save_outreach_row(row)
    return {"ok": True, "row": row}

PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HipHype Lead Finder</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<link rel="alternate icon" href="/favicon.ico">
<style>
:root{--bg:#0f1115;--card:#171a21;--line:#262b36;--txt:#e6e8ee;--mut:#9aa3b2;--acc:#5b8cff;--good:#3fb950}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 system-ui,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--txt)}
header{padding:18px 24px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:12px}
h1{font-size:18px;margin:0}.sub{color:var(--mut);font-size:12px}
.wrap{padding:20px 24px;max-width:1400px;margin:0 auto}
.panel{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin-bottom:18px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
label{display:block;font-size:12px;color:var(--mut);margin-bottom:4px}
input,select{width:100%;padding:8px 10px;background:#0f1218;border:1px solid var(--line);border-radius:7px;color:var(--txt)}
button{background:var(--acc);color:#fff;border:0;padding:10px 18px;border-radius:8px;font-weight:600;cursor:pointer}
button.sec{background:#222836;color:var(--txt);border:1px solid var(--line)}
button:disabled{opacity:.5;cursor:wait}
.row{display:flex;gap:10px;align-items:center;margin-top:14px;flex-wrap:wrap}
table{width:100%;border-collapse:collapse;margin-top:8px}
th,td{text-align:left;padding:9px 10px;border-bottom:1px solid var(--line);vertical-align:top;font-size:13px}
th{color:var(--mut);font-weight:600;position:sticky;top:0;background:var(--card)}
#tbl tr:hover td{background:#1b2029}
a{color:var(--acc);text-decoration:none}a:hover{text-decoration:underline}
.tag{font-size:11px;padding:2px 7px;border-radius:20px;background:#1c2330;color:var(--mut)}
.tag.ok{background:#10301a;color:var(--good)}
.jd{max-width:340px;color:var(--mut);font-size:12px}
.jd summary{cursor:pointer;color:var(--acc)}
.muted{color:var(--mut)}.status{margin-left:8px;color:var(--mut)}
</style></head><body>
<header><h1>HipHype Lead Finder</h1><span class="sub">funded · right-sized · hiring · worldwide</span>
<span style="flex:1"></span><a href="/outreach" class="navlink">LinkedIn Outreach →</a></header>
<div class="wrap">
  <div class="panel" id="resultPanel">
    <div class="row" style="margin-top:0;margin-bottom:10px">
      <button class="sec" onclick="csv()">Export CSV</button>
      <button class="sec" onclick="loadSaved()">Refresh</button>
      <span class="status" id="status"></span>
    </div>
    <div id="count" class="muted"></div>
    <div style="overflow:auto;max-height:80vh"><table id="tbl"></table></div>
  </div>
</div>
<script>
let LEADS=[];
const $=id=>document.getElementById(id);
// Safe DOM helpers (no innerHTML with untrusted data)
function cell(text){const td=document.createElement('td');td.textContent=(text==null?'':String(text));return td;}
function link(text,href){const a=document.createElement('a');a.href=href;a.target='_blank';a.rel='noopener noreferrer';a.textContent=text;return a;}
function safeUrl(u){return (typeof u==='string'&&/^https?:\/\//i.test(u))?u:null;}
function gsearch(dm){return 'https://www.google.com/search?q='+encodeURIComponent(((dm.name||'')+' '+(dm.company||'')+' site:linkedin.com/in').trim());}
function render(){
  $('resultPanel').style.display='block';
  $('count').textContent=LEADS.length+' qualified leads';
  const tbl=$('tbl'); tbl.replaceChildren();
  const head=document.createElement('tr');
  ['Company','What they do','HQ','Size','Funding','Fit','Role','Hiring','Decision-makers (to connect)','Opener'].forEach(c=>{const th=document.createElement('th');th.textContent=c;head.appendChild(th);});
  tbl.appendChild(head);
  for(const l of LEADS){
    const tr=document.createElement('tr');
    // company + website
    const c0=document.createElement('td');const b=document.createElement('b');b.textContent=l.company||'';c0.appendChild(b);
    if(l.website){c0.appendChild(document.createElement('br'));const s=document.createElement('span');s.className='muted';s.textContent=l.website;c0.appendChild(s);}
    tr.appendChild(c0);
    // product / what they do (Z.ai summary)
    const pc=document.createElement('td');pc.className='jd';pc.style.maxWidth='280px';pc.textContent=l.product||'';tr.appendChild(pc);
    tr.appendChild(cell(l.hq));
    tr.appendChild(cell(l.employees));
    // funding (structured: amount / round / date)
    const fc=document.createElement('td');
    if(l.funding_amount){const a=document.createElement('div');a.style.fontWeight='700';a.textContent=l.funding_amount;fc.appendChild(a);}
    if(l.funding_round){const t=document.createElement('span');t.className='tag';t.textContent=l.funding_round;fc.appendChild(t);}
    if(l.funding_date){const dd=document.createElement('div');dd.className='muted';dd.style.fontSize='11px';dd.style.marginTop='3px';dd.textContent=l.funding_date;fc.appendChild(dd);}
    if(!l.funding_amount&&!l.funding_round){fc.textContent=l.funding||'';}
    tr.appendChild(fc);
    // fit (agent score + reason)
    const ftc=document.createElement('td');
    if(l.fit_score){const b=document.createElement('div');b.style.fontWeight='700';b.textContent=l.fit_score+'/10';ftc.appendChild(b);}
    if(l.fit_reason){const s=document.createElement('div');s.className='muted';s.style.fontSize='11px';s.textContent=l.fit_reason;ftc.appendChild(s);}
    tr.appendChild(ftc);
    // role: link + active tag
    const rc=document.createElement('td');const ju=safeUrl(l.job_link);
    if(ju) rc.appendChild(link(l.job_title||'role',ju)); else rc.appendChild(document.createTextNode(l.job_title||''));
    const tag=document.createElement('span');tag.className='tag'+(l.job_active==1?' ok':'');tag.textContent=(l.job_active==1?'active':'check');
    rc.appendChild(document.createTextNode(' '));rc.appendChild(tag);tr.appendChild(rc);
    // hiring momentum: open roles + month-over-month change (replaces posted date)
    const hc=document.createElement('td');
    if(l.jobs_open!=null){
      const n=document.createElement('div');n.style.fontWeight='700';n.textContent=l.jobs_open+(l.jobs_open==1?' role':' roles');hc.appendChild(n);
      const ch=l.jobs_change_m;
      if(ch!=null&&ch!==0){const t=document.createElement('div');t.style.fontSize='11px';t.style.marginTop='3px';t.style.color=ch>0?'var(--good)':'#e06c6c';t.textContent=(ch>0?'▲ +':'▼ ')+ch+'/mo';hc.appendChild(t);}
      else if(ch===0){const t=document.createElement('div');t.className='muted';t.style.fontSize='11px';t.style.marginTop='3px';t.textContent='steady';hc.appendChild(t);}
    } else { hc.textContent='—'; }
    tr.appendChild(hc);
    // decision-makers to connect with (CEO/CTO/VP Eng + LinkedIn + email)
    const dc=document.createElement('td');
    const dms=l.decision_makers||[];
    if(dms.length){
      dms.forEach(dm=>{
        const blk=document.createElement('div');blk.style.marginBottom='8px';
        const nm=document.createElement('div');nm.style.fontWeight='600';nm.textContent=dm.name||'';blk.appendChild(nm);
        if(dm.title){const tt=document.createElement('div');tt.className='muted';tt.style.fontSize='11px';tt.textContent=dm.title;blk.appendChild(tt);}
        const ln=document.createElement('div');ln.style.fontSize='12px';
        const lu=safeUrl(dm.linkedin)||gsearch(dm);
        if(lu) ln.appendChild(link(safeUrl(dm.linkedin)?'Connect':'Find on LinkedIn',lu));
        if(dm.email){ln.appendChild(document.createTextNode(' · '));const em=document.createElement('span');em.textContent=dm.email;ln.appendChild(em);}
        if(dm.phone){ln.appendChild(document.createTextNode(' · '));const ph=document.createElement('span');ph.textContent=dm.phone;ln.appendChild(ph);}
        blk.appendChild(ln);
        const btn=document.createElement('button');btn.className='sec';btn.style.cssText='padding:2px 8px;font-size:11px;margin-top:3px';btn.textContent='Enrich';
        btn.onclick=async()=>{btn.disabled=true;btn.textContent='…';try{const r=await fetch('/api/enrich',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:dm.name,company:dm.company,linkedin:dm.linkedin})});const j=await r.json();ln.textContent='';const lu2=safeUrl(j.linkedin)||gsearch(dm);if(lu2)ln.appendChild(link(j.linkedin?'Connect':'Find on LinkedIn',lu2));if(j.email){ln.appendChild(document.createTextNode(' · '));const em=document.createElement('span');em.textContent=j.email;ln.appendChild(em);}if(j.phone){ln.appendChild(document.createTextNode(' · '));const ph=document.createElement('span');ph.textContent=j.phone;ln.appendChild(ph);}if(!j.email&&!j.phone){const s=document.createElement('span');s.className='muted';s.textContent=' no contact found';ln.appendChild(s);}btn.style.display='none';}catch(e){btn.textContent='err';btn.disabled=false;}};
        blk.appendChild(btn);
        dc.appendChild(blk);
      });
    } else { dc.appendChild(document.createTextNode('—')); }
    tr.appendChild(dc);
    // jd (collapsible, textContent only)
    const oc=document.createElement('td');oc.className='jd';oc.style.maxWidth='320px';
    if(l.opener){const det=document.createElement('details');const sum=document.createElement('summary');sum.textContent='opener';det.appendChild(sum);const div=document.createElement('div');div.style.marginTop='4px';div.textContent=l.opener;det.appendChild(div);oc.appendChild(det);}
    tr.appendChild(oc);
    tbl.appendChild(tr);
  }
}
function csv(){
  if(!LEADS.length){alert('Run a search first');return;}
  const cols=['company','product','fit_score','fit_reason','opener','hq','employees','funding','job_title','job_link','jobs_open','jobs_change_m','buyer','buyer_title','linkedin','email','phone'];
  const esc=v=>'"'+String(v==null?'':v).replace(/"/g,'""')+'"';
  const rows=[cols.join(',')].concat(LEADS.map(l=>cols.map(c=>esc(l[c])).join(',')));
  const blob=new Blob([rows.join('\n')],{type:'text/csv'});
  const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='leads.csv'; a.click();
}
async function loadSaved(){try{const r=await fetch('/api/leads');const j=await r.json();LEADS=j.leads||[];if(LEADS.length){render();$('status').textContent='Loaded '+LEADS.length+' stored leads (auto-updated by the daily 9 AM run).';}else{$('count').textContent='No leads yet — the daily 9 AM run will populate this automatically.';$('status').textContent='';}}catch(e){$('count').textContent='Error loading leads.';}}
loadSaved();
</script></body></html>"""

OUTREACH_PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HipHype Outreach</title>
<link rel="icon" type="image/svg+xml" href="/favicon.svg">
<style>
:root{--bg:#0f1115;--card:#171a21;--line:#262b36;--txt:#e6e8ee;--mut:#9aa3b2;--acc:#5b8cff;--good:#3fb950;--warn:#e0a23f;--bad:#e06c6c}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 system-ui,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--txt)}
header{padding:13px 22px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:12px}
h1{font-size:17px;margin:0}.sub{color:var(--mut);font-size:12px}
a{color:var(--acc);text-decoration:none}a:hover{text-decoration:underline}
button{background:var(--acc);color:#fff;border:0;padding:7px 13px;border-radius:7px;font-weight:600;cursor:pointer;font-size:13px}
button.sec{background:#222836;color:var(--txt);border:1px solid var(--line)}
button.ghost{background:transparent;color:var(--mut);border:1px solid var(--line)}
button:disabled{opacity:.5;cursor:wait}
input,textarea{background:#0f1218;border:1px solid var(--line);border-radius:7px;color:var(--txt);font:inherit;padding:8px 10px;width:100%}
textarea{resize:vertical;min-height:90px;white-space:pre-wrap}
.wrap{padding:16px 22px;max-width:1000px;margin:0 auto}
.tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px}
.tab{padding:7px 12px;border-radius:8px;background:#171a21;border:1px solid var(--line);color:var(--mut);cursor:pointer;font-size:13px}
.tab.on{background:#1d2533;color:var(--txt);border-color:var(--acc)}
.tab .n{background:#2a3142;border-radius:10px;padding:0 6px;margin-left:6px;font-size:11px}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:14px 16px;margin-bottom:12px}
.card h3{margin:0;font-size:15px}
.meta{color:var(--mut);font-size:12px;margin-top:2px}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:10px}
.tag{font-size:11px;padding:2px 8px;border-radius:20px;background:#1c2330;color:var(--mut)}
.tag.fit{color:var(--good)}.tag.stale{background:#3a2118;color:var(--warn)}
.empty{color:var(--mut);padding:36px;text-align:center}
.cc{color:var(--mut);font-size:11px;margin-top:4px}
.quote{border-left:3px solid var(--acc);padding:4px 10px;color:var(--mut);font-size:13px;margin-top:8px;white-space:pre-wrap}
.sched{border:1px solid var(--line);border-radius:8px;padding:8px 10px;margin-top:8px;font-size:13px}
.dt{width:auto;display:inline-block}
</style></head><body>
<header><h1>HipHype Outreach</h1><span class="sub">manual · AI-assisted · you send by hand</span>
<span style="flex:1"></span>
<input id="q" placeholder="Search company / name / role" style="width:230px" oninput="onSearch()">
<button class="sec" onclick="syncLeads(this)">Sync from leads</button>
<a href="/" class="sec" style="padding:7px 13px;border-radius:7px">← Leads</a></header>
<div class="wrap"><div class="tabs" id="tabs"></div><div id="list"></div></div>
<script>
const $=id=>document.getElementById(id);
const TABS=[['to_send','To Send'],['awaiting','Awaiting'],['connected','Connected'],['due','Follow-ups Due'],['scheduled','Scheduled'],['interested','Interested'],['closed','Closed']];
let TAB='to_send',ROWS=[],COUNTS={},MAXF=3,NOW=0,Q='',timer=null;
function el(t,p,kids){const e=document.createElement(t);if(p)for(const k in p){if(k==='text')e.textContent=p[k];else if(k==='cls')e.className=p[k];else if(k.slice(0,2)==='on')e[k]=p[k];else e.setAttribute(k,p[k]);}(kids||[]).forEach(c=>{if(c==null)return;e.appendChild(typeof c==='string'?document.createTextNode(c):c);});return e;}
function safeUrl(u){return (typeof u==='string'&&/^https?:\/\//i.test(u))?u:null;}
function fmtDate(ts){if(!ts)return '';const d=new Date(ts*1000);return d.toLocaleDateString(undefined,{month:'short',day:'numeric'})+', '+d.toLocaleTimeString(undefined,{hour:'2-digit',minute:'2-digit'});}
function toLocalInput(ts){const d=new Date(ts*1000);d.setMinutes(d.getMinutes()-d.getTimezoneOffset());return d.toISOString().slice(0,16);}
function copyBtn(getText){return el('button',{cls:'sec',onclick:async function(){try{await navigator.clipboard.writeText(getText());const o=this.textContent;this.textContent='Copied';setTimeout(()=>this.textContent=o,1200);}catch(e){this.textContent='Copy failed';}},text:'Copy'});}
async function api(action,company,payload){const r=await fetch('/api/outreach/action',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({company:company,action:action,payload:payload||{}})});return r.json();}
async function act(action,company,payload){await api(action,company,payload);load();}
async function load(){try{const r=await fetch('/api/outreach?tab='+encodeURIComponent(TAB)+'&q='+encodeURIComponent(Q));const j=await r.json();ROWS=j.rows||[];COUNTS=j.counts||{};MAXF=j.max_followups||3;NOW=j.now||0;renderTabs();renderList();}catch(e){$('list').textContent='Error loading.';}}
function renderTabs(){const c=$('tabs');c.replaceChildren();TABS.forEach(function(p){const k=p[0],label=p[1];const t=el('div',{cls:'tab'+(k===TAB?' on':''),onclick:()=>{TAB=k;Q='';$('q').value='';load();}},[label]);const n=COUNTS[k]||0;if(n)t.appendChild(el('span',{cls:'n',text:String(n)}));c.appendChild(t);});}
function syncLeads(b){b.disabled=true;b.textContent='Syncing…';fetch('/api/outreach/sync',{method:'POST'}).then(r=>r.json()).then(j=>{b.disabled=false;b.textContent='Sync from leads';load();});}
function onSearch(){clearTimeout(timer);timer=setTimeout(()=>{Q=$('q').value.trim();load();},300);}

function gbtn(label,action,company,cb){const b=el('button',{text:label});b.onclick=async()=>{b.disabled=true;const o=b.textContent;b.textContent='Generating…';try{const j=await api(action,company);cb(j);}catch(e){alert('Generation failed');}b.disabled=false;b.textContent=o;};return b;}
function textArea(val){const ta=el('textarea',{},[]);ta.value=val||'';return ta;}
function charLine(ta){const cc=el('div',{cls:'cc',text:(ta.value||'').length+' chars'});ta.addEventListener('input',()=>cc.textContent=ta.value.length+' chars');return cc;}

function head(row){
  const h=el('div',{},[el('h3',{text:row.company||''}),el('div',{cls:'meta',text:(row.dm_name||'')+(row.dm_title?(' · '+row.dm_title):'')})]);
  if(row.job_title)h.appendChild(el('div',{cls:'meta',text:'hiring: '+row.job_title}));
  const tr=el('div',{cls:'row'},[]);
  if(row.fit_score)tr.appendChild(el('span',{cls:'tag fit',text:'fit '+row.fit_score+'/10'}));
  const lu=safeUrl(row.linkedin_url);
  if(lu)tr.appendChild(el('a',{href:lu,target:'_blank',rel:'noopener noreferrer'},['Open LinkedIn']));
  else tr.appendChild(el('span',{cls:'tag',text:'no LinkedIn URL yet'}));
  if(row.dm_email)tr.appendChild(el('span',{cls:'tag',text:row.dm_email}));
  h.appendChild(tr);
  return h;
}
function repliedBox(row){
  const inp=el('input',{placeholder:'Paste what they said…'});
  const b=el('button',{cls:'sec',text:'They replied',onclick:()=>{act('replied',row.company,{reply:inp.value});}});
  return el('div',{cls:'row'},[inp,b]);
}

function buildCard(row){
  const c=el('div',{cls:'card'},[head(row)]);
  const co=row.company;
  if(TAB==='to_send'){
    if(!safeUrl(row.linkedin_url))
      c.appendChild(el('div',{cls:'row'},[gbtn('Find LinkedIn (enrich)','enrich',co,()=>load())]));
    const box=el('div',{},[]);c.appendChild(box);
    const show=note=>{box.replaceChildren();const ta=textArea(note);box.appendChild(ta);box.appendChild(el('div',{cls:'row'},[copyBtn(()=>ta.value),el('button',{text:'Mark sent',onclick:()=>act('mark_sent',co)}),el('button',{cls:'ghost',text:'Skip',onclick:()=>act('decline',co)})]));box.appendChild(charLine(ta));};
    if(row.connection_note)show(row.connection_note);
    else box.appendChild(el('div',{cls:'row'},[gbtn('Generate connection note','generate_note',co,j=>show(j.text||'')),el('button',{cls:'ghost',text:'Skip',onclick:()=>act('decline',co)})]));
  }
  else if(TAB==='awaiting'){
    const age=row._invite_age_days; const m=el('div',{cls:'meta',text:age==null?'invite sent':('invited '+age+'d ago')});c.appendChild(m);
    const r=el('div',{cls:'row'},[el('button',{text:'Mark connected',onclick:()=>act('mark_connected',co)}),el('button',{cls:'ghost',text:'Declined / ignore',onclick:()=>act('decline',co)})]);
    if(row._stale)r.appendChild(el('span',{cls:'tag stale',text:'stale >'+'30d'}));
    c.appendChild(r);
  }
  else if(TAB==='connected'){
    const first=(row.messages||[]).filter(m=>m.kind==='first').slice(-1)[0];
    const box=el('div',{},[]);c.appendChild(box);
    const show=t=>{box.replaceChildren();const ta=textArea(t);box.appendChild(ta);box.appendChild(el('div',{cls:'row'},[copyBtn(()=>ta.value),el('button',{text:'Mark messaged',onclick:()=>act('mark_messaged',co,{text:ta.value})})]));box.appendChild(charLine(ta));};
    if(first)show(first.text);
    else box.appendChild(el('div',{cls:'row'},[gbtn('Generate first message','generate_first',co,j=>show(j.text||''))]));
    c.appendChild(repliedBox(row));
  }
  else if(TAB==='due'){
    if(row.interested&&row.scheduled){
      const due=(row.scheduled_messages||[]).filter(m=>!m.sent&&(m.sendAt||0)<=NOW).sort((a,b)=>a.sendAt-b.sendAt)[0];
      if(due){const ta=textArea(due.text);c.appendChild(el('div',{cls:'meta',text:'scheduled follow-up due '+fmtDate(due.sendAt)}));c.appendChild(ta);c.appendChild(el('div',{cls:'row'},[copyBtn(()=>ta.value),el('button',{text:'Mark sent',onclick:()=>act('mark_followup_sent',co)})]));}
    } else {
      const n=(row.followup_count||0)+1;
      c.appendChild(el('div',{cls:'meta',text:'auto follow-up #'+n+' of '+MAXF+' due'}));
      const box=el('div',{},[]);c.appendChild(box);
      const show=t=>{box.replaceChildren();const ta=textArea(t);box.appendChild(ta);box.appendChild(el('div',{cls:'row'},[copyBtn(()=>ta.value),el('button',{text:'Mark sent',onclick:()=>act('mark_followup_sent',co,{text:ta.value})})]));box.appendChild(charLine(ta));};
      box.appendChild(el('div',{cls:'row'},[gbtn('Generate follow-up','generate_followup',co,j=>show(j.text||''))]));
      c.appendChild(repliedBox(row));
    }
  }
  else if(TAB==='scheduled'){
    if(row.interested&&row.scheduled){
      (row.scheduled_messages||[]).filter(m=>!m.sent).sort((a,b)=>a.sendAt-b.sendAt).forEach(m=>{
        const due=(m.sendAt||0)<=NOW;
        c.appendChild(el('div',{cls:'sched'},[el('div',{cls:'meta',text:(due?'DUE now · ':'')+fmtDate(m.sendAt)}),el('div',{text:m.text})]));
      });
    } else {
      const n=(row.followup_count||0)+1; const due=(row.followup_due_at||0)<=NOW;
      c.appendChild(el('div',{cls:'sched'},[el('div',{cls:'meta',text:'auto follow-up #'+n+' of '+MAXF+' · '+(due?'DUE now':fmtDate(row.followup_due_at))})]));
    }
    c.appendChild(el('div',{cls:'meta',text:'(actionable in the Follow-ups Due tab)'}));
  }
  else if(TAB==='interested'){
    if(row.last_reply_text)c.appendChild(el('div',{cls:'quote',text:row.last_reply_text}));
    if(row.scheduled&&(row.scheduled_messages||[]).length){
      (row.scheduled_messages||[]).forEach((m,i)=>c.appendChild(el('div',{cls:'sched'},[el('div',{cls:'meta',text:'#'+(i+1)+' · '+(m.sent?'sent':('send '+fmtDate(m.sendAt)))}),el('div',{text:m.text})])));
      c.appendChild(el('div',{cls:'row'},[el('button',{cls:'ghost',text:'Cancel lead',onclick:()=>act('close',co)})]));
    } else {
      const box=el('div',{},[]);c.appendChild(box);
      box.appendChild(el('div',{cls:'row'},[gbtn('Schedule 3 follow-ups','generate_schedule',co,j=>schedEditor(box,row,j.drafts||[])),el('button',{cls:'ghost',text:'Cancel lead',onclick:()=>act('close',co)})]));
    }
  }
  else if(TAB==='closed'){
    c.appendChild(el('div',{cls:'meta',text:'closed ('+(row.status||'')+')'}));
    c.appendChild(el('div',{cls:'row'},[el('button',{cls:'sec',text:'Restore',onclick:()=>act('restore',co)})]));
  }
  return c;
}

function schedEditor(box,row,drafts){
  box.replaceChildren();
  const rows=[];
  for(let i=0;i<3;i++){
    const ta=textArea(drafts[i]||'');
    const dt=el('input',{type:'datetime-local',cls:'dt'});dt.value=toLocalInput(NOW+(i+1)*86400);
    box.appendChild(el('div',{cls:'sched'},[el('div',{cls:'meta',text:'Follow-up #'+(i+1)}),ta,el('div',{cls:'row'},[el('span',{cls:'meta',text:'send at'}),dt]),charLine(ta)]));
    rows.push({ta,dt});
  }
  box.appendChild(el('div',{cls:'row'},[el('button',{text:'Approve & schedule',onclick:()=>{const messages=rows.map(r=>({text:r.ta.value,sendAt:Math.floor(new Date(r.dt.value).getTime()/1000)}));act('schedule',row.company,{messages:messages});}}),el('button',{cls:'ghost',text:'Regenerate',onclick:async()=>{const j=await api('generate_schedule',row.company);schedEditor(box,row,j.drafts||[]);}})]));
}

function renderList(){
  const c=$('list');c.replaceChildren();
  if(!ROWS.length){c.appendChild(el('div',{cls:'empty',text:TAB==='to_send'?'Nothing queued. Click “Sync from leads” to pull in qualified leads.':'Nothing here.'}));return;}
  ROWS.forEach(r=>c.appendChild(buildCard(r)));
}
load();
</script></body></html>"""

# Yellow favicon (rounded square + dark "H" for HipHype), served inline — no binary file needed.
FAVICON = (
    '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">'
    '<rect width="64" height="64" rx="14" fill="#FFD21E"/>'
    '<text x="32" y="47" font-family="Segoe UI,Arial,sans-serif" font-size="44" '
    'font-weight="700" text-anchor="middle" fill="#14161b">H</text>'
    '</svg>'
).encode()

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, ctype, body):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path, qs = parsed.path, urllib.parse.parse_qs(parsed.query)
        if path == "/":
            self._send(200, "text/html; charset=utf-8", PAGE.encode())
        elif path == "/outreach":
            self._send(200, "text/html; charset=utf-8", OUTREACH_PAGE.encode())
        elif path in ("/favicon.svg", "/favicon.ico"):
            self._send(200, "image/svg+xml", FAVICON)
        elif path == "/api/leads":
            self._send(200, "application/json", json.dumps({"leads": _load_leads()}).encode())
        elif path == "/api/outreach":
            tab = (qs.get("tab", ["to_send"])[0]) or "to_send"
            q = (qs.get("q", [""])[0]) or ""
            self._send(200, "application/json", json.dumps(_outreach_payload(tab, q)).encode())
        else:
            self._send(404, "text/plain", b"not found")
    def do_POST(self):
        if self.path == "/api/find-leads":
            n = int(self.headers.get("Content-Length", 0) or 0)
            try:
                cfg = json.loads(self.rfile.read(n) or b"{}")
                leads = find_leads(cfg)
                _persist(leads)
                self._send(200, "application/json", json.dumps({"leads": leads}).encode())
            except Exception as e:
                self._send(500, "application/json", json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/enrich":
            n = int(self.headers.get("Content-Length", 0) or 0)
            try:
                req = json.loads(self.rfile.read(n) or b"{}")
                res = _co_enrich(req.get("name", ""), req.get("company", ""), req.get("linkedin", ""))
                self._send(200, "application/json", json.dumps(res).encode())
            except Exception as e:
                self._send(500, "application/json", json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/outreach/sync":
            try:
                self._send(200, "application/json", json.dumps({"created": _outreach_intake()}).encode())
            except Exception as e:
                self._send(500, "application/json", json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/outreach/action":
            n = int(self.headers.get("Content-Length", 0) or 0)
            try:
                req = json.loads(self.rfile.read(n) or b"{}")
                res = _outreach_action(req.get("company", ""), req.get("action", ""), req.get("payload", {}))
                self._send(200, "application/json", json.dumps(res).encode())
            except Exception as e:
                self._send(500, "application/json", json.dumps({"error": str(e)}).encode())
        else:
            self._send(404, "text/plain", b"not found")

def _run_pipeline_once(tag="CRON"):
    """Full discovery -> qualify -> personalize -> store run (used by --cron and the scheduler)."""
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    try:
        ls = find_leads({})
        _persist(ls)
        queued = _outreach_intake()   # auto-queue qualifying new leads into the outreach worklist
        print("%s %s: %d new leads this run; %d stored total; %d queued for outreach" % (tag, stamp, len(ls), len(_load_leads()), queued), flush=True)
    except Exception as e:
        print("%s %s error: %s" % (tag, stamp, e), flush=True)

def _scheduler():
    """Daily in-process cron: fires _run_pipeline_once at CRON_AT_UTC each day.
    Default 03:30 UTC = 09:00 IST (the user's morning). Set CRON_AT_UTC=HH:MM to change."""
    parts = os.environ.get("CRON_AT_UTC", "03:30").split(":")
    try:
        hh, mm = int(parts[0]), int(parts[1])
    except Exception:
        hh, mm = 3, 30
    print("Daily scheduler ON — runs at %02d:%02d UTC each day." % (hh, mm), flush=True)
    while True:
        now = datetime.datetime.now(datetime.timezone.utc)
        nxt = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if nxt <= now:
            nxt += datetime.timedelta(days=1)
        print("Next daily run: %s UTC" % nxt.strftime("%Y-%m-%d %H:%M"), flush=True)
        while True:
            secs = (nxt - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
            if secs <= 0:
                break
            time.sleep(min(secs, 1800))   # wake at least every 30 min to stay accurate
        _run_pipeline_once("CRON")
        time.sleep(90)                    # avoid double-firing within the same minute

if __name__ == "__main__":
    if not API_KEY:
        print("!! WARNING: DATA_API_KEY env var is not set — searches will fail.")
    import sys
    if "--cron" in sys.argv:
        _run_pipeline_once("CRON")
        sys.exit(0)
    if os.environ.get("CRON_ENABLED", "1") != "0":
        threading.Thread(target=_scheduler, daemon=True).start()
    port = int(os.environ.get("PORT", 8000))
    print("HipHype Lead Finder running on 0.0.0.0:%d" % port)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
