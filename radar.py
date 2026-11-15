#!/usr/bin/env python3
"""Ça te concerne - News Radar V1.

Un passage = lire les flux RSS, détecter les nouveautés, regrouper les sujets,
notifier sur Telegram, répondre aux commandes. Lancé toutes les 5 min par GitHub Actions.
"""
import calendar
import hashlib
import html
import json
import os
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import feedparser

DRY_RUN = bool(os.environ.get("DRY_RUN"))
TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
CHAT_ID = str(os.environ.get("TELEGRAM_CHAT_ID", ""))
DB_PATH = os.environ.get("DB_PATH", "news.db")
PARIS = ZoneInfo("Europe/Paris")
UA = "Mozilla/5.0 (compatible; CaTeConcerneRadar/1.0)"

MAX_AGE_H = 24        # on ignore les articles plus vieux que ça
MAX_SEND = 10         # notifications max par passage
LEVEL_INTERESTING = 5  # score mini pour une notification
LEVEL_URGENT = 8       # score mini pour une alerte urgente
FAIL_WARN_AFTER = 12   # passages consécutifs en échec avant avertissement (~1 h)

# (nom du média, URL du flux, pays par défaut)
FEEDS = [
    ("Franceinfo", "https://www.francetvinfo.fr/titres.rss", "France"),
    ("Franceinfo", "https://www.francetvinfo.fr/monde.rss", "Monde"),
    ("Franceinfo", "https://www.francetvinfo.fr/politique.rss", "France"),
    ("Franceinfo", "https://www.francetvinfo.fr/faits-divers.rss", "France"),
    ("Franceinfo", "https://www.francetvinfo.fr/economie.rss", "France"),
    ("Le Monde", "https://www.lemonde.fr/rss/une.xml", "France"),
    ("Le Monde", "https://www.lemonde.fr/international/rss_full.xml", "Monde"),
    ("Le Figaro", "https://www.lefigaro.fr/rss/figaro_actualites.xml", "France"),
    ("Libération", "https://www.liberation.fr/arc/outboundfeeds/rss-all/collection/accueil-une/?outputType=xml", "France"),
    ("Le Parisien", "https://feeds.leparisien.fr/leparisien/rss", "France"),
    ("20 Minutes", "https://www.20minutes.fr/feeds/rss-une.xml", "France"),
    ("Ouest-France", "https://www.ouest-france.fr/rss/une", "France"),
    ("BFMTV", "https://www.bfmtv.com/rss/news-24-7/", "France"),
    ("France 24", "https://www.france24.com/fr/rss", "Monde"),
    ("RFI", "https://www.rfi.fr/fr/rss", "Monde"),
    ("Courrier international", "https://www.courrierinternational.com/feed/all/rss.xml", "Monde"),
    ("BBC", "https://feeds.bbci.co.uk/news/world/rss.xml", "Monde"),
    ("Al Jazeera", "https://www.aljazeera.com/xml/rss/all.xml", "Monde"),
]


# ---------------------------------------------------------------- texte

def norm(s):
    s = unicodedata.normalize("NFKD", (s or "").lower())
    s = "".join(c for c in s if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def compile_kw(words):
    """'mot' = début de mot ; 'mot$' = mot exact."""
    return [(norm(w.rstrip("$")), w.endswith("$")) for w in words]


def hits(text, kws):
    padded = " " + text + " "
    n = 0
    for k, exact in kws:
        if (" " + k + " " in padded) if exact else (" " + k in padded):
            n += 1
    return n


SPORT = compile_kw([
    "football", "ligue 1", "ligue 2", "ligue des champions", "ligue europa", "rugby", "tennis",
    "roland garros", "wimbledon", "nba", "nfl", "formule 1", "grand prix", "tour de france",
    "mercato", "handball", "basket", "cyclisme", "athletisme", "jeux olympiques", "paralympi",
    "championnat", "coupe du monde de", "psg$", "olympique de marseille", "top 14", "six nations",
    "premier league", "liga$", "serie a", "bundesliga", "match", "buteur", "gardien de but",
    "entraineur", "ski alpin", "biathlon", "natation", "judo", "boxe", "marathon", "coupe de france",
    "pilote", "moto gp", "motogp", "golf$", "olympique lyonnais", "equipe de france de",
])

URGENT = compile_kw([
    "attentat", "explosion", "seisme", "tremblement de terre", "tsunami", "eruption", "morts",
    "mort de", "deces", "assassin", "fusillade", "otages", "attaque", "frappes", "bombard", "missile",
    "guerre", "cessez le feu", "demission", "dissolution", "motion de censure", "49 3", "etat d urgence",
    "evacuation", "incendie", "crash", "accident", "deraill", "inondation", "tempete", "canicule",
    "coup d etat", "mis en examen", "perquisition", "cyberattaque", "panne geante",
])

COUNTRY_FR = compile_kw([
    "france", "francais", "francaise", "macron", "elysee", "matignon", "assemblee nationale", "paris",
    "marseille", "lyon", "prefecture", "sncf", "ratp", "edf", "cgt$", "rn$", "lfi$", "gouvernement",
    "premier ministre", "senat",
])
COUNTRIES = [
    ("Russie", compile_kw(["russie", "russe", "poutine", "moscou", "kremlin", "ukrain", "kiev", "zelensk"])),
    ("Chine", compile_kw(["chine", "chinois", "pekin", "xi jinping", "taiwan", "hong kong"])),
    ("Moyen-Orient", compile_kw(["israel", "gaza", "palestin", "hamas", "hezbollah", "liban", "iran",
                                  "teheran", "syrie", "irak", "yemen", "houthi", "arabie saoudite", "qatar"])),
    ("Afrique", compile_kw(["afrique", "algerie", "maroc", "tunisie", "senegal", "mali$", "niger$",
                             "burkina", "cote d ivoire", "cameroun", "congo", "nigeria", "soudan",
                             "ethiopie", "sahel", "rdc$", "madagascar", "gabon", "tchad", "egypte"])),
    ("États-Unis", compile_kw(["etats unis", "trump", "washington", "maison blanche", "americain",
                                "new york", "pentagone", "congres americain", "republicain", "democrate$"])),
    ("Europe", compile_kw(["union europeenne", "bruxelles", "commission europeenne", "parlement europeen",
                            "allemagne", "berlin", "italie", "espagne", "royaume uni", "londres", "otan"])),
]

CATEGORIES = [
    ("Accident / catastrophe", compile_kw(["accident", "incendie", "seisme", "tremblement", "inondation",
                                           "tempete", "crash", "explosion", "deraill", "catastrophe", "evacuation"])),
    ("Faits divers / justice", compile_kw(["tribunal", "proces", "condamn", "mis en examen", "garde a vue",
                                           "police", "gendarm", "meurtre", "agress", "enquete", "interpell",
                                           "parquet", "procureur", "victime", "justice"])),
    ("Politique", compile_kw(["gouvernement", "ministre", "assemblee", "senat", "depute", "macron", "elysee",
                              "election", "projet de loi", "decret", "reforme", "parlement", "president"])),
    ("Économie", compile_kw(["inflation", "prix", "pouvoir d achat", "salaire", "chomage", "croissance",
                             "bourse", "entreprise", "impot", "taxe", "banque", "carburant", "retraite",
                             "economie", "faillite", "licenci"])),
    ("Transports", compile_kw(["sncf", "train", "avion", "aerien", "ratp", "metro", "autoroute", "aeroport",
                               "transport", "cheminot"])),
    ("Tech / IA", compile_kw(["intelligence artificielle", "ia$", "openai", "chatgpt", "apple", "google",
                              "microsoft", "tesla", "cyber", "numerique", "smartphone", "robot", "tiktok"])),
    ("Société", compile_kw(["ecole", "sante", "hopital", "logement", "immigration", "education", "famille",
                            "religion", "jeunes", "societe", "etudiant", "medecin"])),
]


def clean(s):
    s = re.sub(r"<[^>]+>", " ", s or "")
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


def short_summary(text, title):
    text = clean(text)
    if not text or norm(text) == norm(title):
        return "Résumé non fourni par la source."
    sentences = re.split(r"(?<=[.!?])\s+", text)[:4]
    out = " ".join(sentences)
    return out if len(out) <= 480 else out[:477].rsplit(" ", 1)[0] + "…"


def tokens(title):
    return {w for w in norm(title).split() if len(w) >= 4}


def similar(a, b):
    inter = len(a & b)
    if inter < 3:
        return False
    return inter / min(len(a), len(b)) >= 0.6 and inter / len(a | b) >= 0.3


def detect_country(text, default):
    if hits(text, COUNTRY_FR):
        return "France"
    for name, kws in COUNTRIES:
        if hits(text, kws):
            return name
    return default


def detect_category(title, text, default_country):
    for scope in (norm(title), text):
        for name, kws in CATEGORIES:
            if hits(scope, kws):
                return name
    return "Actualité" if default_country == "France" else "International"


def clean_url(u):
    p = urllib.parse.urlsplit(u)
    return urllib.parse.urlunsplit((p.scheme, p.netloc, p.path.rstrip("/"), "", ""))


# ---------------------------------------------------------------- Telegram

def tg(method, params=None):
    if DRY_RUN:
        print(f"[DRY_RUN] {method}: {params}")
        return {"ok": True, "result": []}
    data = urllib.parse.urlencode(params or {}).encode()
    url = f"https://api.telegram.org/bot{TOKEN}/{method}"
    for attempt in range(3):
        try:
            with urllib.request.urlopen(url, data=data, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            if e.code == 429:
                try:
                    wait = json.loads(body)["parameters"]["retry_after"]
                except Exception:
                    wait = 5
                time.sleep(min(wait, 30))
                continue
            print(f"Telegram HTTP {e.code} sur {method}: {body[:200]}")
            return {"ok": False}
        except Exception as e:  # réseau
            print(f"Telegram erreur sur {method}: {e}")
            time.sleep(2)
    return {"ok": False}


def send(text):
    r = tg("sendMessage", {
        "chat_id": CHAT_ID, "text": text[:4000], "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    })
    return bool(r.get("ok"))


# ---------------------------------------------------------------- base

def init_db(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
    CREATE TABLE IF NOT EXISTS stories(
        id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, country TEXT, category TEXT,
        score INTEGER DEFAULT 0, level INTEGER DEFAULT 0, first_ts INTEGER, last_ts INTEGER);
    CREATE TABLE IF NOT EXISTS items(
        id TEXT PRIMARY KEY, story_id INTEGER, source TEXT, title TEXT, summary TEXT, url TEXT,
        pub_ts INTEGER, pub_known INTEGER, det_ts INTEGER, tokens TEXT);
    CREATE INDEX IF NOT EXISTS idx_items_story ON items(story_id);
    CREATE INDEX IF NOT EXISTS idx_items_pub ON items(pub_ts);
    """)


def meta_get(db, key, default=None):
    r = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return r[0] if r else default


def meta_set(db, key, value):
    db.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
               (key, str(value)))


# ---------------------------------------------------------------- collecte

def fetch(feed):
    name, url, default = feed
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Accept": "application/rss+xml, application/xml, text/xml, */*"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = r.read()
        d = feedparser.parse(data)
        if not d.entries:
            return feed, [], "flux vide ou illisible"
        out = []
        for e in d.entries[:40]:
            link, title = e.get("link"), clean(e.get("title", ""))
            if not link or not title:
                continue
            t = e.get("published_parsed") or e.get("updated_parsed")
            out.append({
                "source": name, "default": default, "title": title, "url": link,
                "summary": e.get("summary", "") or e.get("description", ""),
                "pub": int(calendar.timegm(t)) if t else None,
            })
        return feed, out, None
    except Exception as ex:
        return feed, [], str(ex)[:120]


def collect():
    with ThreadPoolExecutor(8) as ex:
        return list(ex.map(fetch, FEEDS))


def score_story(db, sid):
    rows = db.execute("SELECT source, title, summary FROM items WHERE story_id=?", (sid,)).fetchall()
    sources = {r["source"] for r in rows}
    kw = hits(norm(" ".join(r["title"] for r in rows)), URGENT)
    score = 3 + min(4, 2 * (len(sources) - 1)) + (3 if kw >= 1 else 0) + (1 if kw >= 3 else 0)
    return min(score, 10), len(sources)


def process(db, results, now, first_run=False):
    """Insère les nouveautés. Retourne l'ensemble des sujets touchés."""
    recent = [(r["story_id"], set(r["tokens"].split())) for r in db.execute(
        "SELECT story_id, tokens FROM items WHERE pub_ts >= ?", (now - 36 * 3600,))]
    entries = [e for _, items, _ in results for e in items]
    entries.sort(key=lambda e: e["pub"] or now)
    touched = set()
    for e in entries:
        url = clean_url(e["url"])
        item_id = hashlib.sha1(url.encode()).hexdigest()
        if db.execute("SELECT 1 FROM items WHERE id=?", (item_id,)).fetchone():
            continue
        pub_known = e["pub"] is not None
        # sans date (ex. Le Parisien) : vu pour la 1re fois = maintenant ; au tout 1er passage = ancien
        pub = e["pub"] if pub_known else (now - 23 * 3600 if first_run else now)
        if now - pub > MAX_AGE_H * 3600:
            continue
        text = norm(e["title"] + " " + clean(e["summary"])[:300])
        if hits(text, SPORT) or "/sport" in url.lower():
            continue
        tk = tokens(e["title"])
        sid = next((s for s, t in recent if similar(tk, t)), None)
        if sid is None:
            country = detect_country(text, e["default"])
            cur = db.execute(
                "INSERT INTO stories(title,country,category,first_ts,last_ts) VALUES(?,?,?,?,?)",
                (e["title"], country, detect_category(e["title"], text, e["default"]), pub, pub))
            sid = cur.lastrowid
        else:
            db.execute("UPDATE stories SET last_ts=MAX(last_ts,?) WHERE id=?", (pub, sid))
        db.execute("INSERT INTO items VALUES(?,?,?,?,?,?,?,?,?,?)", (
            item_id, sid, e["source"], e["title"], clean(e["summary"]), url, pub,
            int(pub_known), now, " ".join(sorted(tk))))
        recent.append((sid, tk))
        touched.add(sid)
    return touched


# ---------------------------------------------------------------- messages

def esc(s):
    return html.escape(s or "", quote=False)


def hhmm(ts):
    d = datetime.fromtimestamp(ts, PARIS)
    today = datetime.now(PARIS).date()
    if d.date() == today:
        return d.strftime("%H:%M")
    return ("hier " if (today - d.date()).days == 1 else d.strftime("%d/%m ")) + d.strftime("%H:%M")


def ago(ts):
    m = max(0, int((datetime.now(timezone.utc).timestamp() - ts) // 60))
    if m < 1:
        return "à l'instant"
    if m < 60:
        return f"il y a {m} min"
    return f"il y a {m // 60}h{m % 60:02d}"


def format_story(db, sid, header):
    s = db.execute("SELECT * FROM stories WHERE id=?", (sid,)).fetchone()
    items = db.execute("SELECT * FROM items WHERE story_id=? ORDER BY pub_ts", (sid,)).fetchall()
    best = max(items, key=lambda i: len(i["summary"] or ""))
    seen, sources = set(), []
    for i in items:
        if i["source"] not in seen:
            seen.add(i["source"])
            sources.append(f'• <a href="{html.escape(i["url"], quote=True)}">{esc(i["source"])}</a>')
    first = items[0]
    n = len(seen)
    sources = sources[:6]
    pub_line = f"🕒 Publié : {hhmm(first['pub_ts'])} · détecté : {hhmm(first['det_ts'])}" \
        if first["pub_known"] else f"🕒 Détecté : {hhmm(first['det_ts'])}"
    status = (f"✅ Confirmé par {n} médias" if n >= 2
              else "⏳ Une seule source pour l'instant : à confirmer")
    return (f"{header}\n\n<b>{esc(s['title'])}</b>\n\n"
            f"📍 Pays : {esc(s['country'])}\n🏷 Catégorie : {esc(s['category'])}\n{pub_line}\n\n"
            f"📰 Résumé :\n{esc(short_summary(best['summary'], s['title']))}\n\n"
            f"{status}\n📊 Importance : {s['score']}/10\n\n🔎 Sources :\n" + "\n".join(sources))


def notify(db, touched, silent):
    to_send = []
    for sid in touched:
        score, _ = score_story(db, sid)
        s = db.execute("SELECT level FROM stories WHERE id=?", (sid,)).fetchone()
        new_level = 2 if score >= LEVEL_URGENT else (1 if score >= LEVEL_INTERESTING else 0)
        db.execute("UPDATE stories SET score=? WHERE id=?", (score, sid))
        if new_level > s["level"]:
            if silent:
                db.execute("UPDATE stories SET level=? WHERE id=?", (new_level, sid))
            else:
                to_send.append((score, sid, s["level"], new_level))
    to_send.sort(reverse=True)
    for score, sid, old, new in to_send[:MAX_SEND]:
        if new == 2:
            header = "🚨 ACTUALITÉ IMPORTANTE" if old == 0 else "🚨 MISE À JOUR : désormais très important"
        else:
            header = "🟡 ACTUALITÉ INTÉRESSANTE"
        if send(format_story(db, sid, header)):
            db.execute("UPDATE stories SET level=? WHERE id=?", (new, sid))
            db.commit()
    extra = len(to_send) - MAX_SEND
    if extra > 0:
        for _, sid, _, new in to_send[MAX_SEND:]:
            db.execute("UPDATE stories SET level=? WHERE id=?", (new, sid))
        send(f"ℹ️ {extra} autres actualités détectées. Tape /live pour les voir.")


# ---------------------------------------------------------------- commandes

HELP = ("🤖 <b>Ça te concerne – Radar</b>\n\n/live : dernières actualités\n/france : France\n"
        "/monde : international\n/urgent : alertes majeures\n/economie : économie\n/societe : société")


def story_list(db, title, where="1=1", params=(), hours=12, limit=30):
    since = int(datetime.now(timezone.utc).timestamp()) - hours * 3600
    rows = db.execute(f"""
        SELECT s.*,
          (SELECT url FROM items i WHERE i.story_id=s.id ORDER BY pub_ts LIMIT 1) AS url,
          (SELECT source FROM items i WHERE i.story_id=s.id ORDER BY pub_ts LIMIT 1) AS src,
          (SELECT COUNT(DISTINCT source) FROM items i WHERE i.story_id=s.id) AS n
        FROM stories s WHERE s.last_ts >= ? AND {where} ORDER BY s.last_ts DESC LIMIT ?""",
        (since, *params, limit)).fetchall()
    if not rows:
        return [f"{title}\n\nRien sur les {hours} dernières heures."]
    blocks = []
    for r in rows:
        mark = "🚨" if r["score"] >= LEVEL_URGENT else ("🟡" if r["score"] >= LEVEL_INTERESTING else "▫️")
        blocks.append(f'{mark} <b>{esc(r["title"])}</b>\n🕒 {ago(r["last_ts"])} · {r["score"]}/10 · '
                      f'{r["n"]} source(s) · <a href="{html.escape(r["url"], quote=True)}">{esc(r["src"])}</a>')
    head = f"{title} · du plus récent au plus ancien ({hours} h)"
    chunks = [blocks[i:i + 6] for i in range(0, len(blocks), 6)]
    return [(head + "\n\n" if i == 0 else "") + "\n\n".join(c) for i, c in enumerate(chunks)]


def answer(db, cmd):
    if cmd in ("/start", "/help"):
        return HELP
    if cmd == "/live":
        return story_list(db, "📡 <b>Dernières actualités</b>")
    if cmd == "/france":
        return story_list(db, "🇫🇷 <b>France</b>", "s.country='France'")
    if cmd == "/monde":
        return story_list(db, "🌍 <b>Monde</b>", "s.country!='France'")
    if cmd == "/urgent":
        return story_list(db, "🚨 <b>Alertes majeures</b>", "s.score>=?", (LEVEL_URGENT,), hours=24)
    if cmd == "/economie":
        return story_list(db, "💶 <b>Économie</b>", "s.category='Économie'", hours=24)
    if cmd == "/societe":
        return story_list(db, "👥 <b>Société</b>", "s.category IN ('Société','Faits divers / justice')", hours=24)
    return None


def handle_commands(db):
    offset = int(meta_get(db, "offset", 0))
    r = tg("getUpdates", {"offset": offset, "timeout": 0, "allowed_updates": json.dumps(["message"])})
    for u in r.get("result", []):
        offset = u["update_id"] + 1
        m = u.get("message") or {}
        if str((m.get("chat") or {}).get("id")) != CHAT_ID:
            continue  # bot privé : on ignore les autres
        cmd = (m.get("text") or "").strip().split("@")[0].split(" ")[0].lower()
        reply = answer(db, cmd)
        if reply:
            for part in ([reply] if isinstance(reply, str) else reply):
                send(part)
    meta_set(db, "offset", offset)


# ---------------------------------------------------------------- principal

def main():
    if not DRY_RUN and (not TOKEN or not CHAT_ID):
        sys.exit("TELEGRAM_TOKEN ou TELEGRAM_CHAT_ID manquant")
    now = int(datetime.now(timezone.utc).timestamp())
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    init_db(db)
    first_run = meta_get(db, "seeded") != "1"

    results = collect()
    ok = sum(1 for _, _, err in results if not err)
    for (name, url, _), _, err in results:
        key = "fail:" + url
        if err:
            n = int(meta_get(db, key, 0)) + 1
            meta_set(db, key, n)
            print(f"ECHEC {name} {url}: {err}")
            if n == FAIL_WARN_AFTER:
                send(f"⚠️ Le flux {esc(name)} ne répond plus depuis ~1 h :\n{esc(url)}")
        else:
            meta_set(db, key, 0)

    touched = process(db, results, now, first_run)
    notify(db, touched, silent=first_run)
    if first_run:
        meta_set(db, "seeded", "1")
        bad = [f"{n} ({u.split('/')[2]})" for (n, u, _), _, err in results if err]
        msg = f"✅ Radar démarré : {ok}/{len(results)} flux actifs. Analyse toutes les ~5 minutes.\n\n" + HELP
        if bad:
            msg += "\n\n⚠️ Flux en erreur : " + ", ".join(esc(b) for b in bad)
        send(msg)
    handle_commands(db)
    db.execute("DELETE FROM items WHERE pub_ts < ?", (now - 7 * 86400,))
    db.execute("DELETE FROM stories WHERE id NOT IN (SELECT DISTINCT story_id FROM items)")
    db.commit()
    db.close()
    print(f"OK : {ok}/{len(results)} flux, {len(touched)} sujets touchés")


if __name__ == "__main__":
    main()
