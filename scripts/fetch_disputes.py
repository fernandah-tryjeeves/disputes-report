import json, urllib.request, datetime, os
from collections import defaultdict

METABASE_URL  = os.environ["METABASE_URL"]
METABASE_KEY  = os.environ["METABASE_API_KEY"]
SHEETS_ID     = os.environ["SHEETS_ID"]
CLIENT_ID     = os.environ["GOOGLE_CLIENT_ID"]
CLIENT_SECRET = os.environ["GOOGLE_CLIENT_SECRET"]
REFRESH_TOKEN = os.environ["GOOGLE_REFRESH_TOKEN"]
CARD_ID       = 16937

TRACKER_TABS = ["Nov2025","Dec25","Jan","Feb","March","April"]

WON_STATUSES   = {"dispute won - 1st presentment","dispute won - 2nd presentment"," dispute won - 2nd presentment","dispute won - 2nd presentment"}
PROG_STATUSES  = {"evidence submitted","2nd presentment","pre-arbitration"}
LOSS_STATUSES  = {"rejected by master","rejected - refunded by merchant"}
REF_STATUSES   = {"refunded","client recognized after dispute request"}

TAB_MONTH_MAP  = {"Nov2025":"2025-11","Dec25":"2025-12","Jan":"2026-01","Feb":"2026-02","March":"2026-03","April":"2026-04"}

def get_access_token():
    body = json.dumps({"client_id":CLIENT_ID,"client_secret":CLIENT_SECRET,"refresh_token":REFRESH_TOKEN,"grant_type":"refresh_token"}).encode()
    req  = urllib.request.Request("https://oauth2.googleapis.com/token",data=body,headers={"Content-Type":"application/json"},method="POST")
    with urllib.request.urlopen(req,timeout=30) as r:
        return json.loads(r.read())["access_token"]

def read_sheet(tab, token):
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEETS_ID}/values/{tab}!A:Z"
    req = urllib.request.Request(url, headers={"Authorization":f"Bearer {token}"})
    try:
        with urllib.request.urlopen(req,timeout=30) as r:
            return json.loads(r.read()).get("values",[])
    except Exception as e:
        print(f"  Warning {tab}: {e}")
        return []

def fetch_tracker():
    print("Getting Google OAuth token...")
    token = get_access_token()
    org_tracker   = defaultdict(lambda:{"won":0,"nr":0,"total":0})
    month_tracker = defaultdict(lambda:{"won":0,"won_usd":0.0,"prog":0,"prog_cred_usd":0.0,"lost":0,"lost_usd":0.0,"ref":0,"stop":0,"cred":0,"cred_usd":0.0,"fin_loss":0.0,"nr":0,"not_eligible":0})

    for tab in TRACKER_TABS:
        rows = read_sheet(tab, token)
        print(f"  {tab}: {len(rows)} rows")
        if not rows: continue
        header = [h.strip().lower().replace('\n','').replace(' ','') for h in rows[0]]
        col    = {h:i for i,h in enumerate(header)}
        def g(row, key, default=""):
            i = col.get(key,-1)
            return row[i].strip() if i>=0 and i<len(row) else default

        for row in rows[1:]:
            status    = g(row,"status")
            org_id    = g(row,"org_id")
            date_str  = g(row,"date_of_dispute") or g(row,"dateofthedispute") or ""
            amt_rec   = g(row,"amount_recovered").replace(",",".")
            cred_appl = g(row,"credit_applied").replace(",",".")
            temp_cred = g(row,"temp_credits_issued","No").lower()
            if not org_id or not status: continue
            sl    = status.lower().strip()
            month = date_str[:7] if len(date_str)>=7 else TAB_MONTH_MAP.get(tab,"")

            org_tracker[org_id]["total"] += 1
            if sl in WON_STATUSES:  org_tracker[org_id]["won"] += 1
            if sl == "needs response": org_tracker[org_id]["nr"] += 1

            mt = month_tracker[month]
            if temp_cred == "yes":
                mt["cred"] += 1
                try: mt["cred_usd"] += float(cred_appl) if cred_appl else 0.0
                except: pass

            if sl in WON_STATUSES:
                mt["won"] += 1
                try: mt["won_usd"] += float(amt_rec) if amt_rec else 0.0
                except: pass
            elif sl in PROG_STATUSES:
                mt["prog"] += 1
                try: mt["prog_cred_usd"] += float(cred_appl) if cred_appl else 0.0
                except: pass
            elif sl in LOSS_STATUSES:
                mt["lost"] += 1
                try: mt["lost_usd"] += float(cred_appl) if cred_appl else 0.0
                except: pass
            elif sl in REF_STATUSES:
                mt["ref"] += 1
            else:
                mt["stop"] += 1
            if sl == "needs response":       mt["nr"] += 1
            if "not eligible" in sl:         mt["not_eligible"] += 1

    for m,mt in month_tracker.items():
        mt["fin_loss"] = max(0.0, round(mt["cred_usd"] - mt["won_usd"] - mt["prog_cred_usd"], 2))
        mt["won_usd"]  = round(mt["won_usd"], 2)
        mt["cred_usd"] = round(mt["cred_usd"], 2)

    print(f"Tracker: {len(org_tracker)} orgs, {len(month_tracker)} months")
    return org_tracker, dict(month_tracker)

def fetch_metabase():
    today    = datetime.date.today().isoformat()
    year_ago = (datetime.date.today() - datetime.timedelta(days=365)).isoformat()
    body = json.dumps({"parameters":[
        {"type":"date/single","value":year_ago,"target":["variable",["template-tag","Date_From"]]},
        {"type":"date/single","value":today,   "target":["variable",["template-tag","Date_To"]]}
    ]}).encode()
    req = urllib.request.Request(
        f"{METABASE_URL}/api/card/{CARD_ID}/query/json",
        data=body, headers={"Content-Type":"application/json","x-api-key":METABASE_KEY},
        method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())

def process(rows, org_tracker, month_tracker):
    orgs = defaultdict(lambda:{
        "cases":0,"usd":0.0,"reasons":defaultdict(int),"merchants":defaultdict(int),
        "months":defaultdict(int),"customer":"","card_types":defaultdict(int),
        "mccs":defaultdict(int),"processors":defaultdict(int),"tds":defaultdict(int),
    })
    all_months = set()

    for r in rows:
        oid  = str(r.get("org_id","") or "").strip()
        if not oid: continue
        o = orgs[oid]
        o["customer"]  = r.get("name","") or ""
        o["usd"]      += float(r.get("amount_USD") or 0)
        o["cases"]    += 1
        raw_dt = str(r.get("transactionDateTime","") or "")
        month  = raw_dt[:7] if len(raw_dt)>=7 else ""
        if month: o["months"][month]+=1; all_months.add(month)
        o["reasons"][r.get("dispute_reason","") or "other"] += 1
        merch = r.get("merchant_name","") or ""
        if merch: o["merchants"][merch] += 1
        card = r.get("card_type","") or ""
        if card: o["card_types"][card] += 1
        mcc  = str(r.get("mcc","") or "")
        mcc_d= r.get("mccDescription","") or ""
        if mcc: o["mccs"][f"{mcc}|{mcc_d}"] += 1
        proc = r.get("cardServiceType","") or ""
        if proc: o["processors"][proc] += 1
        tds  = r.get("3DS","") or "No signal"
        o["tds"][tds] += 1

    all_months_sorted = sorted(all_months)
    result = []
    for oid, o in orgs.items():
        total = o["cases"]
        if not total: continue
        t = org_tracker.get(oid)
        win_rate = round(t["won"]/t["total"]*100,1) if t and t["total"]>0 else None
        nr       = t["nr"] if t else 0

        top_r   = sorted(o["reasons"].items(),   key=lambda x:-x[1])
        top_m   = sorted(o["merchants"].items(),  key=lambda x:-x[1])
        top_mcc = sorted(o["mccs"].items(),       key=lambda x:-x[1])
        top_proc= sorted(o["processors"].items(),  key=lambda x:-x[1])
        trend   = [{"month":m,"cases":o["months"].get(m,0)} for m in all_months_sorted]
        active  = sum(1 for t in trend if t["cases"]>0)
        if active < 2: continue

        result.append({
            "org_id":o.org_id if hasattr(o,'org_id') else oid,
            "org_id":oid,
            "customer":o["customer"],
            "total_cases":total,
            "total_usd":round(o["usd"],2),
            "avg_usd":round(o["usd"]/total,2),
            "win_rate":win_rate,
            "needs_response_tracker":nr,
            "raw_statuses":[],
            "top_reasons":[{"label":k,"count":v} for k,v in top_r[:6]],
            "top_merchants":[{"label":k,"count":v} for k,v in top_m[:5]],
            "top_mccs":[{"label":(k.split("|")[0]+" ("+k.split("|")[1]+")" if "|" in k else k),"count":v} for k,v in top_mcc[:6]],
            "top_processors":[{"label":k,"count":v} for k,v in top_proc[:6]],
            "card_types":dict(o["card_types"]),
            "tds":dict(o["tds"]),
            "trend":trend,
        })

    result.sort(key=lambda x:-x["total_cases"])

    # global aggregates
    g_reasons=defaultdict(int); g_cards=defaultdict(int)
    g_monthly=defaultdict(int); g_mccs=defaultdict(int)
    g_procs=defaultdict(int);   g_tds=defaultdict(int)
    for o in result:
        for r in o["top_reasons"]:    g_reasons[r["label"]]+=r["count"]
        for k,v in o["card_types"].items(): g_cards[k]+=v
        for t in o["trend"]:          g_monthly[t["month"]]+=t["cases"]
        for m in o["top_mccs"]:       g_mccs[m["label"]]+=m["count"]
        for p in o["top_processors"]: g_procs[p["label"]]+=p["count"]
        for k,v in o["tds"].items():  g_tds[k]+=v

    mwd = sorted(k for k in g_monthly if g_monthly[k]>0)
    return {
        "generated_at": datetime.datetime.utcnow().isoformat()+"Z",
        "total_cases":  sum(o["total_cases"] for o in result),
        "total_orgs":   len(result),
        "months_range": {"from":mwd[0] if mwd else "","to":mwd[-1] if mwd else ""},
        "month_tracker": month_tracker,
        "global_reasons":   dict(sorted(g_reasons.items(),key=lambda x:-x[1])),
        "global_cards":     dict(g_cards),
        "global_monthly":   {m:g_monthly[m] for m in sorted(g_monthly)},
        "global_mccs":      dict(sorted(g_mccs.items(),key=lambda x:-x[1])[:12]),
        "global_processors":dict(sorted(g_procs.items(),key=lambda x:-x[1])[:10]),
        "global_tds":       dict(sorted(g_tds.items(),key=lambda x:-x[1])),
        "orgs": result,
    }

print("Fetching Metabase Q16937 (last 365 days)...")
rows = fetch_metabase()
print(f"Rows: {len(rows)}")
org_tracker, month_tracker = fetch_tracker()
output = process(rows, org_tracker, month_tracker)
print(f"Orgs: {output['total_orgs']}, Cases: {output['total_cases']}")
with open("disputes.json","w") as f:
    json.dump(output, f, ensure_ascii=False)
print("disputes.json saved")
