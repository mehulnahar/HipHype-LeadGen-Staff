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
def _zai_chat(prompt, max_tokens=130, temp=0.2):
    """One Z.ai GLM chat call -> plain text content (or '' on failure)."""
    if not ZAI_KEY:
        return ""
    body = {"model": "glm-4.5-air", "messages": [{"role": "user", "content": prompt}],
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
            "job_posted": "",
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

PAGE = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>HipHype Lead Finder</title>
<style>
:root{--bg:#0f1115;--card:#171a21;--line:#262b36;--txt:#e6e8ee;--mut:#9aa3b2;--acc:#5b8cff;--good:#3fb950}
*{box-sizing:border-box}body{margin:0;font:14px/1.5 system-ui,Segoe UI,Roboto,sans-serif;background:var(--bg);color:var(--txt)}
header{padding:13px 20px;border-bottom:1px solid var(--line);display:flex;align-items:center;gap:14px}
h1{font-size:16px;margin:0}.sub{color:var(--mut);font-size:12px}
button{background:var(--acc);color:#fff;border:0;padding:7px 14px;border-radius:7px;font-weight:600;cursor:pointer;font-size:13px}
button.sec{background:#222836;color:var(--txt);border:1px solid var(--line)}
a{color:var(--acc);text-decoration:none}a:hover{text-decoration:underline}
.muted{color:var(--mut)}
.tag{font-size:11px;padding:2px 7px;border-radius:20px;background:#1c2330;color:var(--mut)}
.tag.ok{background:#10301a;color:var(--good)}
.layout{display:flex;height:calc(100vh - 51px)}
.left{width:38%;max-width:520px;overflow:auto;border-right:1px solid var(--line)}
.right{flex:1;overflow:auto;padding:22px 28px}
.lead{padding:11px 16px;border-bottom:1px solid var(--line);cursor:pointer;border-left:3px solid transparent}
.lead:hover{background:#1b2029}
.lead.sel{background:#1d2533;border-left-color:var(--acc)}
.lead .co{font-weight:600}
.lead .meta{color:var(--mut);font-size:12px;margin-top:2px}
.fit{font-weight:700;color:var(--good)}
.dm{border:1px solid var(--line);border-radius:8px;padding:10px 12px;margin-bottom:8px}
.dm .nm{font-weight:600}
.kv{margin:16px 0}.kv .k{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.05em;margin-bottom:5px}
.opener{background:#0f1218;border:1px solid var(--line);border-radius:8px;padding:12px;white-space:pre-wrap}
h2{margin:0 0 2px;font-size:20px}
</style></head><body>
<header>
  <h1>HipHype Lead Finder</h1>
  <span class="sub" id="count">loading...</span>
  <span style="flex:1"></span>
  <button class="sec" onclick="loadSaved()">Refresh</button>
  <button class="sec" onclick="csv()">Export CSV</button>
</header>
<div class="layout">
  <div class="left" id="list"></div>
  <div class="right" id="detail"><span class="muted">Select a lead on the left.</span></div>
</div>
<script>
let LEADS=[];
const $=id=>document.getElementById(id);
function safeUrl(u){return (typeof u==='string'&&/^https?:\/\//i.test(u))?u:null;}
function link(text,href){const a=document.createElement('a');a.href=href;a.target='_blank';a.rel='noopener noreferrer';a.textContent=text;return a;}
function gsearch(dm){return 'https://www.google.com/search?q='+encodeURIComponent(((dm.name||'')+' '+(dm.company||'')+' site:linkedin.com/in').trim());}
function kv(label,node){const w=document.createElement('div');w.className='kv';const k=document.createElement('div');k.className='k';k.textContent=label;w.appendChild(k);if(typeof node==='string'){const v=document.createElement('div');v.textContent=node;w.appendChild(v);}else if(node){w.appendChild(node);}return w;}
function renderList(){
  $('count').textContent=LEADS.length+' leads';
  const list=$('list'); list.replaceChildren();
  LEADS.forEach((l,i)=>{
    const d=document.createElement('div'); d.className='lead';
    const co=document.createElement('div'); co.className='co'; co.textContent=l.company||''; d.appendChild(co);
    const m=document.createElement('div'); m.className='meta'; const bits=[];
    if(l.fit_score) bits.push('fit '+l.fit_score+'/10');
    if(l.funding_amount) bits.push(l.funding_amount+' '+(l.funding_round||''));
    if(l.hq) bits.push(l.hq);
    m.textContent=bits.join('  ·  '); d.appendChild(m);
    if(l.job_title){const r=document.createElement('div');r.className='meta';r.textContent='hiring: '+l.job_title;d.appendChild(r);}
    d.onclick=()=>{document.querySelectorAll('.lead').forEach(x=>x.classList.remove('sel'));d.classList.add('sel');showDetail(l);};
    list.appendChild(d);
  });
  if(LEADS.length){ list.firstChild.classList.add('sel'); showDetail(LEADS[0]); }
}
function showDetail(l){
  const D=$('detail'); D.replaceChildren();
  const h=document.createElement('h2'); h.textContent=l.company||''; D.appendChild(h);
  if(l.website){const w=document.createElement('div');w.appendChild(link(l.website, safeUrl(l.website)||('https://'+l.website)));D.appendChild(w);}
  const top=document.createElement('div'); top.style.margin='10px 0';
  if(l.fit_score){const f=document.createElement('span');f.className='fit';f.textContent='Fit '+l.fit_score+'/10';top.appendChild(f);if(l.fit_reason){const s=document.createElement('span');s.className='muted';s.textContent='   '+l.fit_reason;top.appendChild(s);}}
  D.appendChild(top);
  if(l.funding_amount||l.funding) D.appendChild(kv('Funding', l.funding_amount?(l.funding_amount+' · '+(l.funding_round||'')+' · '+(l.funding_date||'')):l.funding));
  if(l.product) D.appendChild(kv('What they do', l.product));
  if(l.job_title){const rc=document.createElement('div');const ju=safeUrl(l.job_link);if(ju)rc.appendChild(link(l.job_title,ju));else rc.textContent=l.job_title;D.appendChild(kv('Hiring (role)', rc));}
  if((l.decision_makers||[]).length){
    const box=document.createElement('div');
    l.decision_makers.forEach(dm=>{
      const c=document.createElement('div');c.className='dm';
      const nm=document.createElement('div');nm.className='nm';nm.textContent=dm.name||'';c.appendChild(nm);
      if(dm.title){const t=document.createElement('div');t.className='muted';t.style.fontSize='12px';t.textContent=dm.title;c.appendChild(t);}
      const ln=document.createElement('div');ln.style.fontSize='13px';ln.style.marginTop='4px';
      const lu=safeUrl(dm.linkedin)||gsearch(dm);
      if(lu)ln.appendChild(link(safeUrl(dm.linkedin)?'Connect':'Find on LinkedIn',lu));
      if(dm.email){ln.appendChild(document.createTextNode(' · '));const e=document.createElement('span');e.textContent=dm.email;ln.appendChild(e);}
      if(dm.phone){ln.appendChild(document.createTextNode(' · '));const p=document.createElement('span');p.textContent=dm.phone;ln.appendChild(p);}
      c.appendChild(ln);
      const btn=document.createElement('button');btn.className='sec';btn.style.cssText='padding:3px 10px;font-size:11px;margin-top:6px';btn.textContent='Enrich (email + phone)';
      btn.onclick=async()=>{btn.disabled=true;btn.textContent='...';try{const r=await fetch('/api/enrich',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:dm.name,company:dm.company,linkedin:dm.linkedin})});const j=await r.json();ln.textContent='';const lu2=safeUrl(j.linkedin)||gsearch(dm);if(lu2)ln.appendChild(link(j.linkedin?'Connect':'Find on LinkedIn',lu2));if(j.email){ln.appendChild(document.createTextNode(' · '));const e=document.createElement('span');e.textContent=j.email;ln.appendChild(e);}if(j.phone){ln.appendChild(document.createTextNode(' · '));const p=document.createElement('span');p.textContent=j.phone;ln.appendChild(p);}if(!j.email&&!j.phone){const s=document.createElement('span');s.className='muted';s.textContent=' no contact found';ln.appendChild(s);}btn.style.display='none';}catch(e){btn.textContent='err';btn.disabled=false;}};
      c.appendChild(btn);box.appendChild(c);
    });
    D.appendChild(kv('Decision-makers (to connect)', box));
  }
  if(l.opener){const o=document.createElement('div');o.className='opener';o.textContent=l.opener;D.appendChild(kv('Outreach opener', o));}
}
function csv(){
  if(!LEADS.length){alert('No data yet');return;}
  const cols=['company','product','fit_score','fit_reason','opener','hq','employees','funding','job_title','job_link','job_posted','buyer','buyer_title','linkedin','email','phone'];
  const esc=v=>'"'+String(v==null?'':v).replace(/"/g,'""')+'"';
  const rows=[cols.join(',')].concat(LEADS.map(l=>cols.map(c=>esc(l[c])).join(',')));
  const blob=new Blob([rows.join('\n')],{type:'text/csv'});
  const a=document.createElement('a'); a.href=URL.createObjectURL(blob); a.download='leads.csv'; a.click();
}
async function loadSaved(){try{const r=await fetch('/api/leads');const j=await r.json();LEADS=j.leads||[];renderList();if(!LEADS.length)$('count').textContent='no leads yet (the daily run will populate)';}catch(e){$('count').textContent='error loading';}}
loadSaved();
</script></body></html>"""

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def _send(self, code, ctype, body):
        self.send_response(code); self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            self._send(200, "text/html; charset=utf-8", PAGE.encode())
        elif self.path == "/api/leads":
            self._send(200, "application/json", json.dumps({"leads": _load_leads()}).encode())
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
        else:
            self._send(404, "text/plain", b"not found")

if __name__ == "__main__":
    if not API_KEY:
        print("!! WARNING: DATA_API_KEY env var is not set — searches will fail.")
    import sys
    if "--cron" in sys.argv:
        try:
            ls = find_leads({})
            _persist(ls)
            print("CRON: %d new leads this run; %d stored total" % (len(ls), len(_load_leads())))
        except Exception as e:
            print("CRON error:", e)
        sys.exit(0)
    port = int(os.environ.get("PORT", 8000))
    print("HipHype Lead Finder running on 0.0.0.0:%d" % port)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()
