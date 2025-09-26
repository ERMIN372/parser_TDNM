import pandas as pd, numpy as np
from pathlib import Path
import argparse
import re
AVG_WORKDAYS_5_2 = 21.7  # среднее рабочих в месяц


COLS = [
    "Должность","Работодатель","ЗП от (т.р.)","ЗП до (т.р.)",
    "Средний совокупный доход при графике 2/2 по 12 часов","В час","Длительность \nсмены",
    "Требуемый\nопыт","Труд-во","График","Частота \nвыплат","Льготы","Обязаности","Ссылка"
]


ROLE_SHEETS = []

def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    import re
    m = {c: c for c in df.columns}
    df = df.rename(columns={c: c.replace("\\n", "\n") for c in df.columns})

    for c in list(df.columns):
        x = c.strip().lower().replace("  ", " ")

        # СНАЧАЛА — совокупный доход (в названии есть "график", поэтому приоритетно)
        if ("совокуп" in x) or ("доход" in x and "12" in x):
            m[c] = "Средний совокупный доход при графике 2/2 по 12 часов"
        elif x in ["должность","позиция","роль"]:
            m[c] = "Должность"
        elif x.startswith("работод"):
            m[c] = "Работодатель"
        elif "зп" in x and "от" in x:
            m[c] = "ЗП от (т.р.)"
        elif "зп" in x and "до" in x:
            m[c] = "ЗП до (т.р.)"
        elif "в час" in x or x == "час":
            m[c] = "В час"
        elif "длительность" in x:
            m[c] = "Длительность \nсмены"
        elif "опыт" in x:
            m[c] = "Требуемый\nопыт"
        elif "труд" in x:
            m[c] = "Труд-во"
        # «График» — только если нет слов «совокуп/доход/12», чтобы не ловить доходную колонку
        elif ("график" in x) and not any(k in x for k in ["совокуп","доход","12"]):
            m[c] = "График"
        elif "частота" in x or "выплат" in x:
            m[c] = "Частота \nвыплат"
        elif "обязан" in x:
            m[c] = "Обязаности"
        elif "льгот" in x or "бенефит" in x:
            m[c] = "Льготы"
        elif "ссылка" in x or "url" in x:
            m[c] = "Ссылка"

    df = df.rename(columns=m)

    # Если внезапно несколько «График*» — оставить первый, остальные удалить
    sched_like = [c for c in df.columns if re.fullmatch(r"График(\.\d+)?", str(c))]
    if len(sched_like) > 1:
        for c in sched_like[1:]:
            df.drop(columns=c, inplace=True, errors="ignore")

    # Гарантируем набор колонок
    for col in COLS:
        if col not in df.columns:
            df[col] = np.nan

    return df[COLS]
def _parse_shift_len_value(val):
    if pd.isna(val): return None
    if isinstance(val, (int,float)):
        v=float(val); return v if 1<=v<=24 else None
    s=str(val).strip()
    m=re.match(r"^'?(?P<a>\d{1,2})\s*-\s*(?P<b>\d{1,2})$", s)
    if m:
        a,b=int(m.group("a")),int(m.group("b"))
        return (a+b)/2.0 if 1<=a<=24 and 1<=b<=24 else None
    m=re.match(r"^'?(?P<x>\d{1,2})$", s)
    if m:
        x=int(m.group("x")); return float(x) if 1<=x<=24 else None
    return None

def _workdays_per_month(schedule_str: str):
    g=(schedule_str or "").lower()
    if "5/2" in g: return 21.0
    if "6/1" in g: return 26.0
    m=re.search(r"\b([1-7])\s*[/\-–]\s*([1-7])\b", g)
    if m:
        on=int(m.group(1)); off=int(m.group(2))
        return 30.0*on/(on+off)
    m=re.search(r"сутк\w*\s*через\s*(\d+)", g)
    if m:
        n=int(m.group(1)); return 30.0/(1+n)
    m=re.search(r"\bвахт\w*\s*([1-9]\d?)\s*[/\-–]\s*([1-9]\d?)\b", g)
    if m:
        a=int(m.group(1)); b=int(m.group(2))
        return 30.0*a/(a+b)
    return None


def _compute(df: pd.DataFrame) -> pd.DataFrame:
    # числовые колонки
    for c in ["ЗП от (т.р.)", "ЗП до (т.р.)", "В час", "Средний совокупный доход при графике 2/2 по 12 часов"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
            df.loc[df[c] <= 0, c] = np.nan

    # месячная середина в рублях
    zpf = df["ЗП от (т.р.)"];
    zpt = df["ЗП до (т.р.)"]
    monthly_tr = ((zpf + zpt) / 2.0).fillna(zpf).fillna(zpt)  # тыс ₽
    monthly_rub = (monthly_tr * 1000.0).where(monthly_tr.notna())

    # часы в месяц = рабочие_дни_в_мес × длительность_смены
    workdays = df["График"].apply(_workdays_per_month)
    shift_len = df["Длительность \nсмены"].apply(_parse_shift_len_value)

    hours_month = pd.Series(index=df.index, dtype="float64")
    mask = workdays.notna() & shift_len.notna()
    hours_month[mask] = workdays[mask] * shift_len[mask]

    # НОВАЯ ЛОГИКА: если «В час» пусто и есть месячная + часы — считаем
    mask_calc = df["В час"].isna() & monthly_rub.notna() & hours_month.notna() & (hours_month > 0)
    df.loc[mask_calc, "В час"] = monthly_rub[mask_calc] / hours_month[mask_calc]

    # Взаимные добивки «час» <-> «12ч смена» (если нужно)
    need_h = df["В час"].isna() & df["Средний совокупный доход при графике 2/2 по 12 часов"].notna()
    df.loc[need_h, "В час"] = df.loc[need_h, "Средний совокупный доход при графике 2/2 по 12 часов"] / 12.0

    need_avg = df["Средний совокупный доход при графике 2/2 по 12 часов"].isna() & df["В час"].notna()
    df.loc[need_avg, "Средний совокупный доход при графике 2/2 по 12 часов"] = df.loc[need_avg, "В час"] * 12.0

    # «Длительность \nсмены» НЕ преобразуем в число — может быть "'8-12"
    for col in ["Обязаности", "Льготы", "Частота \nвыплат", "График", "Труд-во", "Требуемый\nопыт"]:
        if col in df.columns:
            df[col] = df[col].fillna("—")

    return df

def _hours_per_month(row):
    g = str(row.get("График") or "").lower()
    dur_val = _parse_shift_len_value(row.get("Длительность \nсмены"))

    # если duration не задан, попытка угадать
    if dur_val is None:
        dur_val = 12.0 if re.search(r"\b\d\s*/\s*\d\b|12\s*час", g) else 8.0

    # далее как было...
    m = re.search(r"\b([1-7])\s*[/\-–]\s*([1-7])\b", g)
    if m:
        on = int(m.group(1)); off = int(m.group(2))
        workdays = 30.0 * on / (on + off)
        return workdays * dur_val

    if "5/2" in g: return AVG_WORKDAYS_5_2 * dur_val
    if "6/1" in g: return 26.0 * dur_val

    m = re.search(r"сутк\w*\s*через\s*(\d+)", g)
    if m:
        n = int(m.group(1))
        return 30.0/(1+n) * 24.0

    if "вахт" in g:
        return 15.0 * dur_val

    return AVG_WORKDAYS_5_2 * dur_val

def _parse_shift_len_value(val):
    """
    Возвращает число часов для расчёта.
    Если строка вида "'8-12" -> берём среднее (10).
    Если чистое число -> float(val).
    Иначе -> None.
    """
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)):
        v = float(val)
        return v if 1 <= v <= 24 else None
    s = str(val).strip()
    # "'8-12" или "8-12"
    m = re.match(r"^'?(?P<a>\d{1,2})\s*-\s*(?P<b>\d{1,2})$", s)
    if m:
        a, b = int(m.group("a")), int(m.group("b"))
        if 1 <= a <= 24 and 1 <= b <= 24:
            return (a + b) / 2.0
    # одиночное число в строке
    m = re.match(r"^'?(?P<x>\d{1,2})$", s)
    if m:
        x = int(m.group("x"))
        return float(x) if 1 <= x <= 24 else None
    return None


def _write(df: pd.DataFrame, out: Path):
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        base=df.copy()
        if "В час" in base.columns:
            base=base.sort_values(by="В час", ascending=False, na_position="last")
        # гиперссылка в «Должность», сам URL остаётся в «Ссылка»
        book=w.book
        link_fmt=book.add_format({'font_color':'blue','underline':1})
        for sheet_name in ["ОБЩИЙ ЛИСТ"]+ROLE_SHEETS:
            if sheet_name in ROLE_SHEETS:
                sub=base[base["Должность"].astype(str).str.contains(sheet_name, case=False, na=False)].copy()
                if sub.empty: continue
                data=sub
            else:
                data=base
            data.to_excel(w, sheet_name=sheet_name, index=False)
            ws=w.sheets[sheet_name]
            # перезаписать колонку «Должность» гиперссылкой
            col_idx=list(data.columns).index("Должность")
            url_idx=list(data.columns).index("Ссылка")
            for r,(title,url) in enumerate(zip(data["Должность"].astype(str), data["Ссылка"].astype(str)), start=1):
                if isinstance(url,str) and url.startswith("http"):
                    ws.write_url(r, col_idx, url, link_fmt, string=title)
            # автоширина
            for i in range(data.shape[1]):
                col = data.iloc[:, i]
                s = col.dropna().astype(str).str.len()
                if s.empty:
                    width = 12
                else:
                    q = s.quantile(0.9)
                    width = min(max(12, int(q) + 2), 60)
                ws.set_column(i, i, width)


def _load(p: Path) -> pd.DataFrame:
    return pd.read_excel(p) if p.suffix.lower() in [".xlsx",".xls"] else pd.read_csv(p)

def main():
    ap=argparse.ArgumentParser(description="Экспорт под шаблон «Аналитика зп Аэропорт»")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    a=ap.parse_args()
    df=_load(Path(a.input))
    df=_compute(_norm_cols(df))
    _write(df, Path(a.output))
    print("OK:", a.output)

if __name__=="__main__": main()
