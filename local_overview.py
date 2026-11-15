#!/usr/bin/env python3
r"""ダウンロード済みの注釈ファイル（言語・トピック）から、日本語記事の総数とジャンル別件数を数える。
ネットワークは使わない。progress_common.py と同じフォルダ（プロジェクト直下）に置くこと。

  uv run python local_overview.py
  uv run python local_overview.py --months 2024-01:2024-12   # 範囲を絞る
  uv run python local_overview.py --lang jpn                  # 言語ラベルを変える場合

前提のフォルダ構成（プロジェクト直下）:
  infini-news-corpus/annotations/language/year=YYYY/month=MM/part-00000.parquet
  infini-news-corpus/annotations/topic/year=YYYY/month=MM/part-00000.parquet

出力（out/local/）:
  count_by_month.csv     月別の全記事数・日本語記事数
  topics_by_month.csv    月別×IPTCジャンル別の日本語記事数（平均スコア付き）
  topics_total.csv       全期間のジャンル別件数と割合
  topics_by_year.csv     年別×ジャンル別件数
進捗は画面と out/local/progress.log に出る。止まっても、再実行すれば済んだ月を飛ばして続きから進む。

注意:
- 未実行のコードです。lang は CommonLingua のラベル。日本語が0件なら、先頭に出す言語上位を見てラベルを確認。
- 件数には再クロール・転載の重複が含まれる。IPTCは多言語分類器の予測で、日本語での精度は未検証。
- ホスト別の内訳は記事ファイル側にしか無いので、このスクリプトでは出ない。
"""
import argparse
import os
import time
from pathlib import Path

import duckdb

from progress_common import Tracker, log, set_log


def parse_months(spec):
    def ym(s):
        y, m = s.split("-")
        return int(y), int(m)

    if ":" in spec:
        a, b = spec.split(":")
        (y1, m1), (y2, m2) = ym(a), ym(b)
    else:
        (y1, m1) = (y2, m2) = ym(spec)
    out, y, m = [], y1, m1
    while (y, m) <= (y2, m2):
        out.append((y, m))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--root", default="infini-news-corpus")
    p.add_argument("--months", default="2016-08:2026-04")
    p.add_argument("--lang", default="jpn")
    p.add_argument("--out", default="out/local")
    a = p.parse_args()

    root = Path(a.root) / "annotations"
    out = Path(a.out)
    mdir = out / "months"
    mdir.mkdir(parents=True, exist_ok=True)
    set_log(str(out / "progress.log"))
    if not root.exists():
        raise SystemExit(f"{root} が見つかりません。--root を確認してください")

    con = duckdb.connect()
    months = parse_months(a.months)
    log(f"開始: {len(months)}か月分、言語ラベル '{a.lang}'")
    tr = Tracker(len(months))
    first_checked = False

    for y, m in months:
        label = f"{y}-{m:02d}"
        c_out = (mdir / f"count_{label}.csv").as_posix()
        t_out = (mdir / f"topics_{label}.csv").as_posix()
        if os.path.exists(c_out) and os.path.exists(t_out):
            tr.skipped(label)
            continue
        lang_f = (root / "language" / f"year={y}" / f"month={m:02d}" / "part-00000.parquet").as_posix()
        topic_f = (root / "topic" / f"year={y}" / f"month={m:02d}" / "part-00000.parquet").as_posix()
        if not (os.path.exists(lang_f) and os.path.exists(topic_f)):
            tr.done(label, "missing", 0.0)
            continue
        t = time.time()
        if not first_checked:
            top = con.execute(
                f"SELECT lang, count(*) n FROM read_parquet('{lang_f}') GROUP BY 1 ORDER BY n DESC LIMIT 8"
            ).fetchall()
            log(f"  言語ラベルの確認（{label}の上位）: " + ", ".join(f"{k}={n:,}" for k, n in top))
            first_checked = True
        con.execute(f"""
            COPY (SELECT {y} AS year, {m} AS month, count(*) AS n_all,
                         count(*) FILTER (WHERE lang = '{a.lang}') AS n_jpn
                  FROM read_parquet('{lang_f}')) TO '{c_out}' (HEADER, DELIMITER ',')""")
        con.execute(f"""
            COPY (SELECT {y} AS year, {m} AS month, t.iptc_topic,
                         count(*) AS n, round(avg(t.iptc_score), 3) AS avg_score
                  FROM read_parquet('{lang_f}') l
                  JOIN read_parquet('{topic_f}') t USING (warc_record_id)
                  WHERE l.lang = '{a.lang}'
                  GROUP BY 3 ORDER BY n DESC) TO '{t_out}' (HEADER, DELIMITER ',')""")
        tr.done(label, "ok", time.time() - t)

    mg = mdir.as_posix()
    con.execute(f"COPY (SELECT * FROM read_csv('{mg}/count_*.csv') ORDER BY year, month) "
                f"TO '{(out / 'count_by_month.csv').as_posix()}' (HEADER, DELIMITER ',')")
    con.execute(f"COPY (SELECT * FROM read_csv('{mg}/topics_*.csv') ORDER BY year, month, n DESC) "
                f"TO '{(out / 'topics_by_month.csv').as_posix()}' (HEADER, DELIMITER ',')")
    con.execute(f"""COPY (SELECT iptc_topic, sum(n) AS n,
                           round(100.0 * sum(n) / sum(sum(n)) OVER (), 2) AS pct
                    FROM read_csv('{mg}/topics_*.csv') GROUP BY 1 ORDER BY n DESC)
                    TO '{(out / 'topics_total.csv').as_posix()}' (HEADER, DELIMITER ',')""")
    con.execute(f"""COPY (SELECT year, iptc_topic, sum(n) AS n
                    FROM read_csv('{mg}/topics_*.csv') GROUP BY 1, 2 ORDER BY 1, n DESC)
                    TO '{(out / 'topics_by_year.csv').as_posix()}' (HEADER, DELIMITER ',')""")

    log("---- 年別 ----")
    for y, n_all, n_jpn in con.execute(
        f"SELECT year, sum(n_all), sum(n_jpn) FROM read_csv('{mg}/count_*.csv') GROUP BY 1 ORDER BY 1"
    ).fetchall():
        pct = 100 * n_jpn / n_all if n_all else 0
        log(f"{y}: 全記事 {n_all:>12,} / 日本語 {n_jpn:>10,} ({pct:.2f}%)")
    tot = con.execute(f"SELECT sum(n_all), sum(n_jpn) FROM read_csv('{mg}/count_*.csv')").fetchone()
    log(f"合計: 全記事 {tot[0]:,} / 日本語 {tot[1]:,}")
    log("---- 日本語記事のジャンル（全期間） ----")
    for k, n, pc in con.execute(
        f"SELECT iptc_topic, n, pct FROM read_csv('{(out / 'topics_total.csv').as_posix()}')"
    ).fetchall():
        log(f"{k:<28}{n:>12,}  {pc:>5}%")
    log(f"出力: {out.as_posix()}/")


if __name__ == "__main__":
    main()
