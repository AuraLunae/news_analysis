#!/usr/bin/env python3
"""Infini-News の注釈ファイル（言語・トピック）だけで、日本語記事の総数とジャンル別件数を数える。

記事本文のファイルは読まない。注釈ファイルは月ごとに1つで、全記事分の行がある。

  pip install duckdb
  # 1) 全117か月の「月別 全記事数 / 日本語記事数」。言語列だけを読むので軽い想定
  python infini_overview.py count
  # 2) 日本語記事のIPTCジャンル別件数（月ごと）。2つの注釈ファイルを結合するので重い
  python infini_overview.py topics --months 2024-01
  python infini_overview.py topics --months 2024-01:2024-12

注意:
- 未実行です（作成環境ではネットワーク不可）。まず count と topics の1か月分で時間を測ってください。
- lang は CommonLingua のラベル（日本語は 'jpn' のはず。0件ならラベルを確認）。
- 件数には再クロール・転載の重複が含まれます。ホスト別の内訳はこのスクリプトでは出ません
  （ホストは記事ファイル側の列なので、infini_fetch.py の audit を使う）。
- IPTCのトピックは多言語分類器による予測で、日本語での精度は未検証です。
- HF_TOKEN 環境変数があれば認証付きで読みます。
"""
import argparse
import os
import sys
import time

import duckdb

ROOT = "hf://datasets/ruggsea/infini-news-corpus/annotations"


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    token = os.environ.get("HF_TOKEN")
    if token:
        con.execute(f"CREATE SECRET hf (TYPE HUGGINGFACE, TOKEN '{token}')")
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
    os.makedirs(args.out, exist_ok=True)
    out = os.path.join(args.out, "count_by_month.csv")
    t = time.time()
    sql = f"""
    COPY (
      SELECT year, month,
             count(*) AS n_all,
             count(*) FILTER (WHERE lang = 'jpn') AS n_jpn
      FROM read_parquet('{ROOT}/language/year=*/month=*/part-00000.parquet',
                        hive_partitioning = true)
      GROUP BY 1, 2 ORDER BY 1, 2
    ) TO '{out}' (HEADER, DELIMITER ',')
    """
    connect().execute(sql)
    print(f"ok {time.time() - t:.0f}s -> {out}")


def topics(args):
    os.makedirs(args.out, exist_ok=True)
    con = connect()
    for y, m in parse_months(args.months):
        out = os.path.join(args.out, f"topics_{y}-{m:02d}.csv")
        if os.path.exists(out):
            print(f"skip {out}")
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
        try:
            con.execute(sql)
            print(f"{y}-{m:02d}: ok {time.time() - t:.0f}s -> {out}")
        except Exception as e:
            print(f"{y}-{m:02d}: NG {e}", file=sys.stderr)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("count")
    c.add_argument("--out", default="out")
    tp = sub.add_parser("topics")
    tp.add_argument("--months", required=True, help="2024-01 または 2024-01:2024-12")
    tp.add_argument("--out", default="out")
    a = p.parse_args()
    count(a) if a.cmd == "count" else topics(a)
