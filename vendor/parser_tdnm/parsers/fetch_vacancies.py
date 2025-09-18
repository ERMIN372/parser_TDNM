# parsers/fetch_vacancies.py
import time, argparse, pandas as pd, re, urllib.parse, html
from typing import List, Dict, Any, Tuple, Optional

try:  # pragma: no cover - import shim for script execution
    from ..constants import DEFAULT_HH_SEARCH_FIELD
except ImportError:  # noqa: F401 - fallback for running as standalone script
    import importlib
    import sys
    from pathlib import Path

    current_dir = Path(__file__).resolve().parent.parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    DEFAULT_HH_SEARCH_FIELD = importlib.import_module("constants").DEFAULT_HH_SEARCH_FIELD

try:  # pragma: no cover - зависимость должна ставиться вместе с ботом
    import requests
except ImportError:  # pragma: no cover
    class _RequestsStub:
        def get(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise RuntimeError(
                "requests package is required for network fetching. Install dependencies."
            )

    requests = _RequestsStub()  # type: ignore[assignment]

# ---- optional bs4 for HTML (hh + gorodrabot) ----
try:
    from bs4 import BeautifulSoup
    _HAS_BS4 = True
except ImportError:
    BeautifulSoup = None
    _HAS_BS4 = False

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "ru-RU,ru;q=0.9"
}

TEMPLATE_COLS = [
    "Должность","Работодатель","ЗП от (т.р.)","ЗП до (т.р.)",
    "Средний совокупный доход при графике 2/2 по 12 часов","В час","Длительность \nсмены",
    "Требуемый\nопыт","Труд-во","График","Частота \nвыплат","Льготы","Обязаности","Ссылка"
]

# ================= РОЛЕВЫЕ ФИЛЬТРЫ ПО НАЗВАНИЮ =================
FILTERS = {
    "повар": {
        "inc": [r"\bповар\b", r"\bшеф-?повар\b", r"\bсу-?шеф\b",
                r"\bпиццамейкер\b", r"\bсушист\b", r"\bкондитер\b", r"\bпекар\b"],
        "exc": [r"\bаттракци", r"\bкол-?центр\b", r"\bcall[- ]?центр\b",
                r"\bпродаж", r"\bкассир\b", r"\bстанк", r"\bазс\b", r"\bзаправк"]
    },
    "повар_холодного_цеха": {
        "inc": [r"\bповар\b.*\bхолодн\w*\b", r"\bповар холодного цеха\b", r"\bхолодный цех\b"],
        "exc": [r"\bаттракци", r"\bкассир\b", r"\bстанк", r"\bcall[- ]?центр\b", r"\bофициант\b", r"\bбармен\b"]
    },
    "повар_горячего_цеха": {
        "inc": [r"\bповар\b.*\bгоряч\w*\b", r"\bповар горячего цеха\b", r"\bгорячий цех\b"],
        "exc": [r"\bаттракци", r"\bкассир\b", r"\bстанк", r"\bcall[- ]?центр\b", r"\bофициант\b", r"\bбармен\b"]
    },
    "оператор_доставки": {
        "inc": [
            r"\bоператор\W{0,3}достав", r"\bоператор заказов\b",
            r"\bдиспетчер[- ]?достав", r"\bкоординатор достав",
            r"\bоператор (?:пвз|пункта выдачи)\b", r"\bпункт выдачи\b",
            r"\bсборщик заказов\b"
        ],
        "exc": [
            r"\bаттракци", r"\bкассир\b", r"\bпродаж", r"\bстанк",
            r"\bcall[- ]?центр\b", r"\bофициант\b", r"\bбармен\b"
        ]
    },
    "кассир": {
        "inc": [
            r"\bкассир\b", r"\bстарший\s+кассир\b",
            r"\bкассир[- ]операционист\b", r"\bпродавец[- ]кассир\b",
            r"\bкассир[- ]консультант\b", r"\bкассир[- ]смены\b",
        ],
        "exc": [
            r"\bбариста\b", r"\bадминистратор\b", r"\bбухгалтер\w*\b",
            r"\bоператор\b", r"\bcall[- ]?центр\b",
            r"\bофициант\b", r"\bбармен\b", r"\bповар\b",
            r"^(?=.*продавец[- ]?консультант)(?!.*кассир).*$",
        ],
    },
    "менеджер_разработки_продукта": {
        "inc": [
            r"\bменеджер\b.*\bразработк\w*\b.*\bпродукт",
            r"\bменеджер\b.*\bразработк\w*\b.*\bменю\b",
            r"\bменеджер\b.*\bблюд\w*\b",
            r"\bменеджер\s*r\W?&\W?d\b",
            r"\bproduct\s*development\b",
        ],
        "exc": [
            r"\bпродакт\b", r"\bproduct\s*manager\b",
            r"\bIT\b|\bайти\b|\bdigital\b|\bsoftware\b|\bприложен|\bПО\b",
        ],
    },
    "бариста": {
        "inc": [
            r"\bбариста\b",
            r"\bстарший\s+бариста\b",
            r"\bbarista\b",
            r"\bкофе[йи]\w*\s*мастер\b",   # кофемастер, кофейный мастер
        ],
        "exc": [
            r"\bофициант\b",
            r"\bповар\b",
            r"\bбармен\b",
            r"\bкассир\b",
            r"\bадминистратор\b",
            r"\bоператор\b",
            r"\bcall[- ]?центр\b",
            r"\bпромоутер\b",
            r"\bкурьер\b",
            r"\bпродаж",                   # менеджер по продажам и пр.
            r"\bаттракци",                 # мусор из парков
        ],
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

# ================= УТИЛИТЫ ПАРСИНГА ТЕКСТА =================
def _strip_html(s: Optional[str]) -> str:
    if not s: return ""
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()

def extract_comp(text: str) -> Tuple[Optional[float], Optional[float]]:
    """Вернёт (hour, shift12). Если есть почасовая — смена 12ч считается всегда."""
    t = (text or "").lower()
    def num(m): return float(re.sub(r"[^\d]","", m.group(1))) if m else None
    m_hour  = re.search(r"(\d[\d\s]{2,})\s*(?:₽|руб)\s*(?:/|за)?\s*час", t, re.I)
    m_shift = re.search(r"(\d[\d\s]{3,})\s*(?:₽|руб).{0,25}(?:смен[аы]|12\s*час)", t, re.I)
    hour  = num(m_hour)
    shift = num(m_shift)
    if hour:
        span = m_hour.span()
        win = t[max(0,span[0]-20):min(len(t), span[1]+20)]
        if re.search(r"(мес|месяц|год)", win): hour = None
    if hour and not shift: shift = hour * 12.0
    if shift and not hour: hour = shift / 12.0
    return hour, shift

# строго: только числовые графики
NUM_WORD = {"сутки":1,"день":1,"один":1,"одна":1,"два":2,"две":2,"три":3,"четыре":4,"пять":5,"шесть":6,"семь":7}
SCHED_NUM_RE = re.compile(r"\b([1-9]\d?)\s*[/\-–xх×]\s*([1-9]\d?)\b", re.I)
def _words_pair(t: str) -> Optional[str]:
    m = re.search(rf"\b({'|'.join(NUM_WORD)})\s+через\s+({'|'.join(NUM_WORD)})\b", t, re.I)
    if not m: return None
    a = NUM_WORD.get(m.group(1).lower()); b = NUM_WORD.get(m.group(2).lower())
    if a and b: return f"{a}/{b}"
    return None

def extract_schedule_strict(text: str, sched_src: Optional[str]=None) -> Optional[str]:
    t = (text or "") + " " + (sched_src or "")
    t = t.lower().replace("–","-").replace("х","x")
    vals = []
    for m in SCHED_NUM_RE.finditer(t):
        vals.append(f"{int(m.group(1))}/{int(m.group(2))}")
    wp = _words_pair(t)
    if wp: vals.append(wp)
    # вахта 15/15
    for m in re.finditer(r"\bвахт\w*\s*([1-9]\d?)\s*[/\-–xх×]\s*([1-9]\d?)\b", t, re.I):
        vals.append(f"{int(m.group(1))}/{int(m.group(2))}")
    out, seen = [], set()
    for v in vals:
        if v not in seen:
            seen.add(v); out.append(v)
    return ", ".join(out) if out else None

# добор графика и часов из HTML hh
def _extract_schedule_from_html(url: str, timeout: float = 15.0) -> Tuple[Optional[str], Optional[float]]:
    if not (_HAS_BS4 and isinstance(url,str) and url.startswith("http")):
        return None, None
    try:
        r = requests.get(url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "lxml")

        txts = []
        for sel in [
            "[data-qa='vacancy-view-employment-mode']",
            "[data-qa='vacancy-view-raw__schedule']",
            "[data-qa='vacancy-view-raw__workingschedule']",
            "[data-qa='vacancy-view-employment-type']",
            "[data-qa='vacancy-view-raw__main-info']",
            "[data-qa='vacancy-view-employment-mode-item']",
        ]:
            for el in soup.select(sel):
                txts.append(el.get_text(" ", strip=True))

        for label in soup.find_all(string=re.compile(r"(График|График работы|Рабочие часы|Смена)", re.I)):
            parent = getattr(label, "parent", None)
            s = " ".join(parent.stripped_strings) if parent else str(label)
            txts.append(s)

        blob = " | ".join(txts).lower()
        blob = html.unescape(blob).replace("–","-").replace("х","x")

        graph = None
        m = SCHED_NUM_RE.search(blob)
        if m: graph = f"{int(m.group(1))}/{int(m.group(2))}"
        if not graph:
            graph = _words_pair(blob)

        mh = re.search(r"(?:длительность|рабочие\s*часы|смена)\D{0,12}(\d{1,2})\s*час", blob)
        hours = float(mh.group(1)) if mh else None

        return graph, hours
    except Exception:
        return None, None

def extract_pay_frequency(text: str) -> Optional[str]:
    if not text: return None
    t = text.lower()
    if re.search(r"еженедел|каждую неделю|раз в неделю|weekly", t): return "Еженедельно"
    if re.search(r"2 раза в месяц|два раза в месяц|аванс", t):     return "2 раза в месяц"
    if re.search(r"ежемесяч|раз в месяц|monthly", t):               return "Ежемесячно"
    return None

def extract_employment_type(text: str, employment_name: Optional[str] = None) -> Optional[str]:
    t = (text or "").lower(); e = (employment_name or "").lower()
    if re.search(r"гпх|гражданско-правов|самозанят|подряд|аутстаф", t): return "ГПХ"
    if re.search(r"по тк|трудов|официальн|оформление по тк|белая зп", t): return "ТК"
    if re.search(r"полная|частичная|полный|частичный", e): return "ТК"
    return None

SECTION_HEADS = ["обязанности","что делать","чем предстоит заниматься","задачи"]
NEXT_HEADS = ["требования","условия","мы предлагаем","о компании","график","контакты","оформление","что мы предлагаем"]
def extract_responsibilities(html_or_text: str, fallback: Optional[str] = None) -> Optional[str]:
    text = _strip_html(html_or_text); low = text.lower()
    start = None
    for h in SECTION_HEADS:
        for sep in (":"," :","\n"):
            i = low.find(h + sep)
            if i != -1:
                start = i + len(h) + len(sep); break
        if start is not None: break
    if start is None:
        lines = [l.strip(" -•—\t") for l in text.splitlines() if l.strip().startswith(("—","-","•"))]
        return ("; ".join([l for l in lines if l])[:3000] or fallback or (text[:3000] if text else None))
    tail = text[start:]; end = len(tail); low_tail = tail.lower()
    for nh in NEXT_HEADS:
        for sep in (":","\n"):
            j = low_tail.find(nh + sep)
            if j != -1: end = min(end, j)
    body = tail[:end]
    lines = [re.sub(r"^[\s\-•—]+", "", l).strip() for l in body.splitlines()]
    lines = [l for l in lines if l]
    return ("; ".join(lines)[:3000]) if lines else fallback

def extract_shift_len(text: str) -> Optional[tuple]:
    """
    Возвращает одну из форм:
      - ("text", "'8-12")  # строка для Excel, чтобы не превратилось в дату
      - ("num", 12.0)      # если в тексте явно одна длина смены (12 часов)
      - None               # если ничего не нашли
    """
    t = (text or "").lower().replace("–", "-")
    # 8-12, 10-11 и т.п. (с/без слова "час")
    m = re.search(r"\b(\d{1,2})\s*-\s*(\d{1,2})(?:\s*час\w*)?\b", t)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if 1 <= a <= 24 and 1 <= b <= 24:
            return ("text", f"'{a}-{b}")
    # "с 8 до 12 часов"
    m = re.search(r"\bс\s*(\d{1,2})\s*до\s*(\d{1,2})\s*час", t)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if 1 <= a <= 24 and 1 <= b <= 24:
            return ("text", f"'{a}-{b}")
    # "12-часовая смена", "смена 12 часов"
    m = re.search(r"\b(\d{1,2})\s*[- ]?\s*час(?:овая)?\b|\bсмена\s*(\d{1,2})\s*час", t)
    if m:
        v = m.group(1) or m.group(2)
        h = float(v)
        if 1 <= h <= 24:
            return ("num", h)
    return None


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

# =================== HH.RU ===================
def hh_search(query: str, area: int, pages: int, per_page: int, pause: float, search_in: str) -> List[Dict[str, Any]]:
    items=[]
    for page in range(pages):
        p={"text":query,"area":area,"page":page,"per_page":per_page,"only_with_salary":"false"}
        if search_in in ("name","description","company_name","everything"):
            p["search_field"]=search_in
        r=requests.get("https://api.hh.ru/vacancies", params=p, headers=HEADERS, timeout=20)
        if r.status_code!=200: break
        data=r.json(); items+=data.get("items",[])
        if page>=data.get("pages",0)-1: break
        time.sleep(pause)
    return items

def hh_details(vac_id: str) -> dict:
    r=requests.get(f"https://api.hh.ru/vacancies/{vac_id}", headers=HEADERS, timeout=20)
    return r.json() if r.status_code==200 else {}

def map_hh(items: List[Dict[str, Any]], pause_detail: float = 0.2) -> List[Dict[str, Any]]:
    rows = []
    for v in items:
        vid = v.get("id")
        name = v.get("name")
        employer = (v.get("employer") or {}).get("name")
        url = v.get("alternate_url") or v.get("url")

        salary = v.get("salary") or {}
        cur = salary.get("currency") or "RUR"
        to_tr = lambda x: round(x / 1000.0, 1) if (x is not None and x > 0 and cur == "RUR") else None

        exp = (v.get("experience") or {}).get("name")
        empl_src = (v.get("employment") or {}).get("name")
        sched_src = (v.get("schedule") or {}).get("name")  # используем как текст-источник, не доверяем «гибкий»

        snip = v.get("snippet") or {}
        resp_snip = snip.get("responsibility") or ""
        reqs_snip = snip.get("requirement") or ""
        short = f"{resp_snip} {reqs_snip}"

        det = hh_details(vid) if vid else {}
        descr_html = det.get("description") or ""
        descr_txt = _strip_html(descr_html) or short

        hour, shift = extract_comp(descr_txt)
        graph = extract_schedule_strict(descr_txt, sched_src=None)  # собирает ВСЕ варианты, напр. "5/2, 4/3"

        # длительность смены из текста
        sl = extract_shift_len(descr_txt)
        if sl:
            if sl[0] == "text":
                shift_len = sl[1]  # "'8-12"
            else:
                shift_len = sl[1]  # 12.0
        else:
            shift_len = 12.0 if (hour or shift) else None

        # HTML-добор (только если не нашли)
        if (not graph or shift_len is None) and isinstance(url, str) and url.startswith("http"):
            g_html, hours_html = _extract_schedule_from_html(url)
            if not graph and g_html:
                graph = g_html  # вернёт "5/2, 4/3" если оба найдены
            if shift_len is None and hours_html:
                shift_len = float(hours_html)



        # итоговые значения
        shift12_out = shift if shift is not None else (hour * 12.0 if hour else None)

        pay   = extract_pay_frequency(descr_txt)
        employ = extract_employment_type(descr_txt, employment_name=empl_src)
        duties = extract_responsibilities(descr_html or descr_txt, fallback=resp_snip or reqs_snip)
        bens = pick_benefits(descr_txt)

        rows.append({
            "Должность": name,
            "Работодатель": employer,
            "ЗП от (т.р.)": to_tr(salary.get("from")),
            "ЗП до (т.р.)": to_tr(salary.get("to")),
            "Средний совокупный доход при графике 2/2 по 12 часов": shift12_out,
            "В час": hour,
            "Длительность \nсмены": shift_len,
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

# =================== gorodrabot.ru ===================
def _text(node) -> str:
    return re.sub(r"\s+"," ", node.get_text(strip=True)) if node else ""

def gorodrabot_search(query: str, city: str, pages: int, pause: float) -> List[Dict[str, Any]]:
    if not _HAS_BS4: return []
    items=[]
    q = urllib.parse.quote(query); c = urllib.parse.quote(city)
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

                items.append({"title": title, "employer": emp or None,
                              "salary_raw": sal_raw or None, "desc": desc or None,
                              "url": link})
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

def map_gorodrabot(rows_in: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    mapped = []
    for r in rows_in:
        desc = r.get("desc") or ""
        sal_raw = r.get("salary_raw") or ""
        combo = f"{sal_raw} {desc}"

        sal_from, sal_to = _rub_to_tr(sal_raw)
        hour, shift = extract_comp(combo)
        graph = extract_schedule_strict(combo, sched_src=None)
        pay = extract_pay_frequency(combo)
        duties = extract_responsibilities(desc, fallback=None)
        bens = pick_benefits(desc)

        mapped.append({
            "Должность": r.get("title"),
            "Работодатель": r.get("employer"),
            "ЗП от (т.р.)": sal_from,
            "ЗП до (т.р.)": sal_to,
            "Средний совокупный доход при графике 2/2 по 12 часов": (shift if shift is not None else (hour*12.0 if hour else None)),
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

# =================== СВОД И ВЫВОД ===================
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
    ap = argparse.ArgumentParser(description="Парсер вакансий: hh.ru + gorodrabot.ru (строгий график)")
    ap.add_argument("--query", required=True)
    ap.add_argument("--area", type=int, default=1)                   # hh регион (1=Москва)
    ap.add_argument("--city", default="Москва")                      # gorodrabot город
    ap.add_argument("--role", default=None, help="ключ из FILTERS")
    ap.add_argument("--pages", type=int, default=3)
    ap.add_argument("--per-page", dest="per_page", type=int, default=50)
    ap.add_argument("--per_page", dest="per_page", type=int, help="alias", metavar="N")
    ap.add_argument("--pause", type=float, default=0.6)
    ap.add_argument("--search-in", dest="search_in", default=DEFAULT_HH_SEARCH_FIELD,
                    help="name|description|company_name|everything")
    ap.add_argument("--search_in", dest="search_in", help="alias")
    ap.add_argument("--site", choices=["hh", "gorodrabot", "both"], default="both")
    ap.add_argument("--out_csv", required=True)
    a = ap.parse_args()

    INC_RE, EXC_RE = _compile_filters(a.role or "")

    rows_hh = []
    if a.site in ("hh", "both"):
        hh_items = hh_search(a.query, a.area, a.pages, a.per_page, a.pause, a.search_in)
        rows_hh = map_hh(hh_items)

    # GorodRabot
    rows_gr = []
    if _HAS_BS4 and a.site in ("gorodrabot", "both"):
        gr_items = gorodrabot_search(a.query, a.city, a.pages, a.pause)
        rows_gr = map_gorodrabot(gr_items)

    rows = rows_hh + rows_gr

    # фильтр по заголовку
    rows = [r for r in rows if keep_by_title(str(r.get("Должность","")), INC_RE, EXC_RE)]

    df = to_df(rows)
    df.to_csv(a.out_csv, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(df)} rows -> {a.out_csv}")

if __name__ == "__main__":
    main()
