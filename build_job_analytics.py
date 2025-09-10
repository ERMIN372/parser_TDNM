import pandas as pd, numpy as np
from pathlib import Path
import argparse

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

def _compute(df: pd.DataFrame) -> pd.DataFrame:
    df["Средний совокупный доход при графике 2/2 по 12 часов"] = pd.to_numeric(
        df["Средний совокупный доход при графике 2/2 по 12 часов"], errors="coerce"
    )
    df["В час"] = pd.to_numeric(df["В час"], errors="coerce")

    need_h = df["В час"].isna() & df["Средний совокупный доход при графике 2/2 по 12 часов"].notna()
    df.loc[need_h, "В час"] = df.loc[need_h, "Средний совокупный доход при графике 2/2 по 12 часов"] / 12.0

    need_avg = df["Средний совокупный доход при графике 2/2 по 12 часов"].isna() & df["В час"].notna()
    df.loc[need_avg, "Средний совокупный доход при графике 2/2 по 12 часов"] = df.loc[need_avg, "В час"] * 12.0

    if "Длительность \nсмены" in df.columns:
        df["Длительность \nсмены"] = df["Длительность \nсмены"].where(df["Длительность \nсмены"].notna(), 12)

    # заполняем пропуски точечно (без «График»)
    for col in ["Обязаности", "Льготы", "Частота \nвыплат", "График", "Труд-во", "Требуемый\nопыт"]:
        if col in df.columns:
            df[col] = df[col].fillna("—")

    return df

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
