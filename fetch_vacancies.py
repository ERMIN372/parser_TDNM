# parsers/fetch_vacancies.py
import time, argparse, requests, pandas as pd, re, urllib.parse
from typing import List, Dict, Any, Tuple, Optional

# bs4 для gorodrabot.ru (опционально)
try:
    from bs4 import BeautifulSoup
    _HAS_BS4 = True
except ImportError:
    BeautifulSoup = None
    _HAS_BS4 = False

HEADERS = {"User-Agent": "job-analytics-script/1.1"}

TEMPLATE_COLS = [
    "Должность","Работодатель","ЗП от (т.р.)","ЗП до (т.р.)",
    "Средний совокупный доход при графике 2/2 по 12 часов","В час","Длительность \nсмены",
    "Требуемый\nопыт","Труд-во","График","Частота \nвыплат","Льготы","Обязаности","Ссылка"
]

# ---------- РОЛЕВОЙ ФИЛЬТР ПО НАЗВАНИЮ ----------
FILTERS = {
    "оператор доставки": {
        "inc": [r"\bоператор\W{0,3}достав", r"\bоператор заказов\b",
                r"\bдиспетчер[- ]?достав", r"\bкоординатор достав",
                r"\bоператор пункта выдачи\b", r"\bсборщик заказов\b"],
        "exc": [r"\bаттракци", r"\bкассир\b", r"\bстанк", r"\bоператор пункта выдачи\b",
                r"\bоператор пвз\b", r"\bсборщик заказов\b",
                r"\bcall[- ]?центр\b", r"\bофициант\b", r"\bбармен\b"]
    }
}
def _compile_filters(role: str):
    cfg = FILTERS.get(role, {})
    inc = cfg.get("inc", [r".*"])
    exc = cfg.get("exc", [])
    INC = re.compile("|".join(inc), re.I)
    EXC = re.compile("|".join(exc), re.I) if exc else None
    return INC, EXC
def keep_by_title(title: str, INC, EXC) -> bool:
    t = title or ""
    if EXC and EXC.search(t): return False
    return bool(INC.search(t))

# ---------- УТИЛИТЫ ПАРСИНГА ТЕКСТА ----------
def _strip_html(s: Optional[str]) -> str:
    if not s: return ""
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def _num(s: str) -> Optional[float]:
    if not s: return None
    d = re.sub(r"[^\d]", "", s)
    return float(d) if d else None

HOUR_PATS = [
    r"(\d[\d\s]{2,})\s*(?:₽|руб)\s*(?:/|за)?\s*час",
    r"часовая\s*ставка\s*(\d[\d\s]{2,})",
]
SHIFT12_PATS = [
    r"(\d[\d\s]{3,})\s*(?:₽|руб).{0,25}(?:смен[аы]|12\s*час)",
    r"за\s*смену\s*12\s*час\w*\s*(\d[\d\s]{3,})\s*(?:₽|руб)",
]
def parse_hour_shift(text: str) -> Tuple[Optional[float], Optional[float]]:
    t = (text or "").lower()
    hour = None; shift = None
    for p in HOUR_PATS:
        m = re.search(p, t, flags=re.I)
        if m:
            v = _num(m.group(1))
            if v: hour = v; break
    for p in SHIFT12_PATS:
        m = re.search(p, t, flags=re.I)
        if m:
            v = _num(m.group(1))
            if v: shift = v; break
    if hour and not shift: shift = hour * 12
    if shift and not hour: hour = shift / 12.0
    return hour, shift

SCHEDULE_REGEX = re.compile(
    r"\b([1-7])\s*[/\-–]\s*([1-7])\b|сутки\s*через\s*\d+|день\s*через\s*\d+|вахт\w+",
    re.I
)
def extract_schedule(text: str) -> Optional[str]:
    if not text: return None
    vals = []
    for m in SCHEDULE_REGEX.finditer(text):
        v = m.group(0).lower().replace("–","-").strip()
        mm = re.match(r"^\s*([1-7])\s*[-–/]\s*([1-7])\s*$", v)
        vals.append(f"{mm.group(1)}/{mm.group(2)}" if mm else v)
    out, seen = [], set()
    for v in vals:
        if v not in seen:
            seen.add(v); out.append(v)
    return ", ".join(out) if out else None

def extract_pay_frequency(text: str) -> Optional[str]:
    if not text: return None
    t = text.lower()
    if any(k in t for k in ["еженедел", "каждую неделю", "раз в неделю", "weekly"]):
        return "Еженедельно"
    if any(k in t for k in ["2 раза в месяц", "два раза в месяц", "аванс", "аванс+зарплата"]):
        return "2 раза в месяц"
    if any(k in t for k in ["ежемесяч", "раз в месяц", "monthly"]):
        return "Ежемесячно"
    return None

def extract_employment_type(text: str, employment_name: Optional[str] = None) -> Optional[str]:
    t = (text or "").lower()
    e = (employment_name or "").lower()
    if any(k in t for k in ["гпх", "гражданско-правов", "самозанят", "подряд", "аутстаф"]):
        return "ГПХ"
    if any(k in t for k in ["по тк", "трудов", "официальн", "оформление по тк", "белая зп"]):
        return "ТК"
    if any(k in e for k in ["полная", "частичная", "полный", "частичный"]):
        return "ТК"
    return None

SECTION_HEADS = ["обязанности","что делать","чем предстоит заниматься","задачи"]
NEXT_HEADS = ["требования","условия","мы предлагаем","о компании","график","контакты","оформление","что мы предлагаем"]
def extract_responsibilities(html_or_text: str, fallback: Optional[str] = None) -> Optional[str]:
    text = _strip_html(html_or_text)
    low = text.lower()
    start = None
    for h in SECTION_HEADS:
        for sep in (":"," :","\n"):
            i = low.find(h + sep)
            if i != -1:
                start = i + len(h) + len(sep)
                break
        if start is not None: break
    if start is None:
        lines = [l.strip(" -•—\t") for l in text.splitlines() if l.strip().startswith(("—","-","•"))]
        return ("; ".join([l for l in lines if l])[:3000] or fallback or (text[:3000] if text else None))
    tail = text[start:]
    end = len(tail); low_tail = tail.lower()
    for nh in NEXT_HEADS:
        for sep in (":","\n"):
            j = low_tail.find(nh + sep)
            if j != -1: end = min(end, j)
    body = tail[:end]
    lines = [re.sub(r"^[\s\-•—]+", "", l).strip() for l in body.splitlines()]
    lines = [l for l in lines if l]
    return ("; ".join(lines)[:3000]) if lines else fallback

BENEFITS = ["дмс","медицинская страховка","страхование","питание","бесплатное питание","корпоративное питание",
            "форма","униформа","спецодежда","премии","бонус","бонусы","подарки","скидки","обучение",
            "проезд","оплата проезда","жилье","жильё","общежитие","развозка","транспорт","кофе","чай"]
def pick_benefits(text: str) -> Optional[str]:
    t = (text or "").lower()
    out, seen = [], set()
    for kw in BENEFITS:
        if kw in t and kw not in seen:
            seen.add(kw); out.append(kw.upper() if kw=="дмс" else kw)
    return ", ".join(out) if out else None

# ---------- HH.RU ----------
def hh_search(query: str, area: int, pages: int, per_page: int, pause: float) -> List[Dict[str, Any]]:
    items=[]
    for page in range(pages):
        p={"text":query,"area":area,"page":page,"per_page":per_page,
           "only_with_salary":"false","search_field":"name"}  # искать только в названии
        r=requests.get("https://api.hh.ru/vacancies", params=p, headers=HEADERS, timeout=20)
        if r.status_code!=200: break
        data=r.json(); items+=data.get("items",[])
        if page>=data.get("pages",0)-1: break
        time.sleep(pause)
    return items

def hh_details(vac_id: str) -> dict:
    r=requests.get(f"https://api.hh.ru/vacancies/{vac_id}", headers=HEADERS, timeout=20)
    return r.json() if r.status_code==200 else {}

def map_hh(items: List[Dict[str, Any]], pause_detail: float=0.2) -> List[Dict[str, Any]]:
    rows=[]
    for v in items:
        vid = v.get("id")
        name = v.get("name")
        employer = (v.get("employer") or {}).get("name")
        url = v.get("alternate_url") or v.get("url")
        salary = v.get("salary") or {}
        cur = salary.get("currency") or "RUR"
        to_tr = lambda x: round(x/1000.0,1) if (x is not None and x > 0 and cur=="RUR") else None

        exp = (v.get("experience") or {}).get("name")
        empl_src = (v.get("employment") or {}).get("name")
        sched_src = (v.get("schedule") or {}).get("name")
        snip = v.get("snippet") or {}
        resp_snip = snip.get("responsibility") or ""
        reqs_snip = snip.get("requirement") or ""
        short = f"{resp_snip} {reqs_snip}"

        det = hh_details(vid) if vid else {}
        descr_html = det.get("description") or ""
        descr_txt = _strip_html(descr_html) or short

        hour, shift = parse_hour_shift(descr_txt)
        graph = extract_schedule(descr_txt) or sched_src
        pay   = extract_pay_frequency(descr_txt)
        employ = extract_employment_type(descr_txt, employment_name=empl_src)
        duties = extract_responsibilities(descr_html or descr_txt, fallback=resp_snip or reqs_snip)
        bens = pick_benefits(descr_txt)

        rows.append({
            "Должность": name,
            "Работодатель": employer,
            "ЗП от (т.р.)": to_tr(salary.get("from")),
            "ЗП до (т.р.)": to_tr(salary.get("to")),
            "Средний совокупный доход при графике 2/2 по 12 часов": shift,
            "В час": hour,
            "Длительность \nсмены": 12 if (hour or shift) else None,
            "Требуемый\nопыт": exp or None,
            "Труд-во": employ,
            "График": graph,
            "Частота \nвыплат": pay,
            "Льготы": bens,
            "Обязаности": duties,
            "Ссылка": url
        })
        time.sleep(pause_detail)
    return rows

# ---------- gorodrabot.ru ----------
def _text(node) -> str:
    return re.sub(r"\s+"," ", node.get_text(strip=True)) if node else ""

def gorodrabot_search(query: str, city: str, pages: int, pause: float) -> List[Dict[str, Any]]:
    if not _HAS_BS4: return []
    items=[]
    q = urllib.parse.quote(query)
    c = urllib.parse.quote(city)
    for p in range(1, pages+1):
        url = f"https://gorodrabot.ru/{q}?l={c}&p={p}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code!=200: break
            soup = BeautifulSoup(r.text,"lxml")
            for a in soup.select("a[href*='/vacancy/'], a[href*='/jobs/']"):
                title = a.get("title") or a.get("aria-label") or _text(a)
                href = a.get("href") or ""
                if not href: continue
                link = href if href.startswith("http") else urllib.parse.urljoin("https://gorodrabot.ru", href)
                cont = a.find_parent(["article","div","li"]) or a.parent

                emp = _text(cont.select_one(".company, .vacancy-company, [class*='company']"))
                sal_raw = _text(cont.select_one(".salary, .vacancy-salary, [class*='salary']"))
                desc = _text(cont.select_one(".description, .vacancy-description, [class*='desc']"))

                items.append({"title": title, "employer": emp or None, "salary_raw": sal_raw or None,
                              "desc": desc or None, "url": link})
        except Exception:
            break
        time.sleep(pause)
    return items

def _rub_to_tr(s: Optional[str]) -> Tuple[Optional[float],Optional[float]]:
    if not s: return None, None
    sums = re.findall(r"(\d[\d\s]{3,})\s*(?:₽|руб)", s.lower())
    vals=[]
    for part in sums:
        v = int(re.sub(r"\D","", part))
        if 1000 <= v <= 10_000_000:
            vals.append(v)
    if not vals: return None, None
    return round(min(vals)/1000.0,1), round(max(vals)/1000.0,1)

def _hour_from_text(s: Optional[str]) -> Tuple[Optional[float],Optional[float]]:
    return parse_hour_shift(s or "")

def map_gorodrabot(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    mapped=[]
    for r in rows:
        combo = ((r.get("salary_raw") or "") + " " + (r.get("desc") or ""))
        sal_from, sal_to = _rub_to_tr(r.get("salary_raw"))
        hour, shift = _hour_from_text(combo)
        graph = extract_schedule(combo)
        pay   = extract_pay_frequency(combo)
        duties = extract_responsibilities(r.get("desc") or "", fallback=None)
        bens = pick_benefits(r.get("desc") or "")
        mapped.append({
            "Должность": r.get("title"),
            "Работодатель": r.get("employer"),
            "ЗП от (т.р.)": sal_from,
            "ЗП до (т.р.)": sal_to,
            "Средний совокупный доход при графике 2/2 по 12 часов": shift,
            "В час": hour,
            "Длительность \nсмены": 12 if (hour or shift) else None,
            "Требуемый\nопыт": None,
            "Труд-во": None,
            "График": graph,
            "Частота \nвыплат": pay,
            "Льготы": bens,
            "Обязаности": duties,
            "Ссылка": r.get("url")
        })
    return mapped

# ---------- СВОД И ВЫВОД ----------
def to_df(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for c in TEMPLATE_COLS:
        if c not in df.columns: df[c] = None
    df = df[TEMPLATE_COLS]
    if "Ссылка" in df.columns:
        df = df.drop_duplicates(subset=["Ссылка"], keep="first")
    df = df.drop_duplicates(subset=["Должность","Работодатель"], keep="first")
    return df

def main():
    ap = argparse.ArgumentParser(description="Парсер вакансий: hh.ru + gorodrabot.ru")
    ap.add_argument("--query", required=True)
    ap.add_argument("--area", type=int, default=1)      # hh.ru регион
    ap.add_argument("--city", default="Москва")         # gorodrabot город
    ap.add_argument("--role", default="повар", help="роль фильтра: 'повар' | 'оператор_доставки' и т.п.")
    ap.add_argument("--pages", type=int, default=3)
    ap.add_argument("--per_page", type=int, default=50) # hh.ru
    ap.add_argument("--pause", type=float, default=0.6)
    ap.add_argument("--out_csv", required=True)
    a = ap.parse_args()

    INC_RE, EXC_RE = _compile_filters(a.role)

    # hh.ru
    hh_items = hh_search(a.query, a.area, a.pages, a.per_page, a.pause)
    rows = map_hh(hh_items)

    # gorodrabot.ru
    if _HAS_BS4:
        gr_items = gorodrabot_search(a.query, a.city, a.pages, a.pause)
        rows += map_gorodrabot(gr_items)

    # фильтр по заголовку
    rows = [r for r in rows if keep_by_title(str(r.get("Должность","")), INC_RE, EXC_RE)]

    df = to_df(rows)
    df.to_csv(a.out_csv, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(df)} rows -> {a.out_csv}")

if __name__ == "__main__":
    main()
