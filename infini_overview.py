#!/usr/bin/env python3
r"""Infini-News の注釈ファイル（言語・トピック）だけで、日本語記事の総数とジャンル別件数を数える。
同じフォルダに progress_common.py が必要。

  uv add duckdb
  $env:HF_TOKEN="hf_..."      # 無いとHugging Faceのレート制限(429)にすぐ当たる
  uv run python infini_overview.py count
  uv run python infini_overview.py count --months 2024-01:2024-12
  uv run python infini_overview.py topics --months 2024-01

進捗: 画面に「[何件目/全体] 月 結果 秒数 | 経過 | 残り約」を出す。同じ内容を out/progress.log にも保存する。
      別のPowerShellで  Get-Content out\progress.log -Wait  を実行すると、途中経過を見続けられる。

注意:
- 未実行のコードです。止まっても、同じコマンドを再実行すれば済んだ月を飛ばして続きから進む。
- lang は CommonLingua のラベル（日本語は 'jpn' のはず。0件ならラベルを確認）。
- 件数には再クロール・転載の重複が含まれる。IPTCトピックの日本語での精度は未検証。
"""
import argparse
import os
import time

import duckdb

from progress_common import Tracker, log, run_sql, set_log

ROOT = "hf://datasets/ruggsea/infini-news-corpus/annotations"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    token = os.environ.get("HF_TOKEN")
    if token:
        con.execute(f"CREATE SECRET hf (TYPE HUGGINGFACE, TOKEN '{token}')")
        log("HF_TOKEN を使用します")
    else:
        log("警告: HF_TOKEN が未設定です。レート制限に当たりやすくなります")
    return con


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


def count(args):
    d = os.path.join(args.out, "count")
    os.makedirs(d, exist_ok=True)
    months = parse_months(args.months)
    log(f"count 開始: {len(months)}か月分")
    con = connect()
    tr = Tracker(len(months))
    for y, m in months:
        label = f"{y}-{m:02d}"
        out = os.path.join(d, f"count_{label}.csv")
        if os.path.exists(out):
            tr.skipped(label)
            continue
        t = time.time()
        f = f"{ROOT}/language/year={y}/month={m:02d}/part-00000.parquet"
        sql = f"""
        COPY (
          SELECT {y} AS year, {m} AS month,
                 count(*) AS n_all,
                 count(*) FILTER (WHERE lang = 'jpn') AS n_jpn
          FROM read_parquet('{f}')
        ) TO '{out}' (HEADER, DELIMITER ',')
        """
        r = run_sql(con, sql, label)
        tr.done(label, r, time.time() - t)
    merged = os.path.join(args.out, "count_by_month.csv")
    con.execute(
        f"COPY (SELECT * FROM read_csv('{d}/count_*.csv') ORDER BY year, month) "
        f"TO '{merged}' (HEADER, DELIMITER ',')"
    )
    n_all, n_jpn = con.execute(
        f"SELECT sum(n_all), sum(n_jpn) FROM read_csv('{merged}')"
    ).fetchone()
    log(f"統合: {merged}")
    log(f"合計: 全記事 {n_all:,} 件 / 日本語 {n_jpn:,} 件（集計できた月のみ）")


def topics(args):
    os.makedirs(args.out, exist_ok=True)
    months = parse_months(args.months)
    log(f"topics 開始: {len(months)}か月分")
    con = connect()
    tr = Tracker(len(months))
    for y, m in months:
        label = f"{y}-{m:02d}"
        out = os.path.join(args.out, f"topics_{label}.csv")
        if os.path.exists(out):
            tr.skipped(label)
            continue
        t = time.time()
        lang = f"{ROOT}/language/year={y}/month={m:02d}/part-00000.parquet"
        topic = f"{ROOT}/topic/year={y}/month={m:02d}/part-00000.parquet"
        sql = f"""
        COPY (
          SELECT t.iptc_topic, count(*) AS n, round(avg(t.iptc_score), 3) AS avg_score
          FROM read_parquet('{lang}') l
          JOIN read_parquet('{topic}') t USING (warc_record_id)
          WHERE l.lang = 'jpn'
          GROUP BY 1 ORDER BY n DESC
        ) TO '{out}' (HEADER, DELIMITER ',')
        """
        r = run_sql(con, sql, label)
        tr.done(label, r, time.time() - t)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("count")
    c.add_argument("--months", default="2016-08:2026-04")
    c.add_argument("--out", default="out")
    tp = sub.add_parser("topics")
    tp.add_argument("--months", required=True, help="2024-01 または 2024-01:2024-12")
    tp.add_argument("--out", default="out")
    a = p.parse_args()
    set_log(os.path.join(a.out, "progress.log"))
    count(a) if a.cmd == "count" else topics(a)
