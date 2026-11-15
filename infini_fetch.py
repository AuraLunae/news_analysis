#!/usr/bin/env python3
"""Infini-News (ruggsea/infini-news-corpus v2) から日本語ニュースを取得する。

使い方（まず audit、結果を見てから extract）:
  pip install duckdb
  # 1) カバレッジ監査: 本文列を読まず、ホスト別の件数・公開日欠損・重複を数える
  python infini_fetch.py audit --months 2024-01
  # 2) 本抽出: 対象ホストの全記事を、本文の先頭だけ残して保存する
  python infini_fetch.py extract --months 2024-01 --hosts hosts.txt --limit 2000   # 動作確認
  python infini_fetch.py extract --months 2016-08:2026-04 --hosts hosts.txt        # 全期間

注意:
- パーティションは「クロール月」であり、記事の公開月ではない。公開日は publish_date 列（欠損あり）。
- 重複（再クロール・転載）は残っている。分母は text_xxhash64 などで別途集計する。
- 抽出時は分母を保つため、キーワードによる絞り込みはしない。
- 未実行のコードです（作成環境ではネットワーク不可）。最初は1か月＋--limit で試してください。
- HF_TOKEN 環境変数があれば認証付きで読む（匿名だとレート制限にかかることがある）。
"""
import argparse
import os
import sys
import time

import duckdb

BASE = "hf://datasets/ruggsea/infini-news-corpus/data/year={y}/month={m:02d}/part-*.parquet"


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


def connect():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    token = os.environ.get("HF_TOKEN")
    if token:
        con.execute(f"CREATE SECRET hf (TYPE HUGGINGFACE, TOKEN '{token}')")
    return con


def run_with_retry(con, sql, tries=3):
    for i in range(tries):
        try:
            con.execute(sql)
            return True
        except Exception as e:  # ネットワーク断・レート制限など
            print(f"  失敗({i + 1}/{tries}): {e}", file=sys.stderr)
            time.sleep(10 * (i + 1))
    return False


def audit(args):
    os.makedirs(args.out, exist_ok=True)
    con = connect()
    for y, m in parse_months(args.months):
        out = os.path.join(args.out, f"audit_{y}-{m:02d}.csv")
        if os.path.exists(out):
            print(f"skip {out}")
            continue
        t = time.time()
        sql = f"""
        COPY (
          SELECT url_hostname,
                 count(*) AS n,
                 count(DISTINCT text_xxhash64) AS n_distinct_text,
                 count(*) FILTER (WHERE publish_date IS NULL OR publish_date = '') AS n_no_pubdate,
                 round(avg(text_length)) AS avg_text_len
          FROM read_parquet('{BASE.format(y=y, m=m)}')
          WHERE language_iso639_3 = 'jpn' OR url_tld LIKE '%jp'
          GROUP BY 1 ORDER BY n DESC
        ) TO '{out}' (HEADER, DELIMITER ',')
        """
        ok = run_with_retry(con, sql)
        print(f"{y}-{m:02d}: {'ok' if ok else 'NG'} {time.time() - t:.0f}s -> {out}")


def extract(args):
    os.makedirs(args.out, exist_ok=True)
    with open(args.hosts, encoding="utf-8") as f:
        hosts = [h.strip() for h in f if h.strip() and not h.startswith("#")]
    cond = " OR ".join(
        f"url_hostname = '{h}' OR url_hostname LIKE '%.{h}'" for h in hosts
    )
    lim = f"LIMIT {int(args.limit)}" if args.limit else ""
    con = connect()
    for y, m in parse_months(args.months):
        out = os.path.join(args.out, f"news_{y}-{m:02d}.parquet")
        if os.path.exists(out):
            print(f"skip {out}")
            continue
        tmp = out + ".part"
        t = time.time()
        sql = f"""
        COPY (
          SELECT url, url_hostname, warc_record_id, warc_date, publish_date,
                 title, description, categories, tags, language_iso639_3,
                 text_length, text_xxhash64,
                 substr(text, 1, {int(args.head)}) AS text_head
          FROM read_parquet('{BASE.format(y=y, m=m)}')
          WHERE {cond}
          {lim}
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
        """
        ok = run_with_retry(con, sql)
        if ok:
            os.replace(tmp, out)
            print(f"{y}-{m:02d}: ok {time.time() - t:.0f}s {os.path.getsize(out) / 1e6:.1f}MB")
        else:
            print(f"{y}-{m:02d}: NG（再実行すると続きから）")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("audit", "extract"):
        s = sub.add_parser(name)
        s.add_argument("--months", required=True, help="2024-01 または 2016-08:2026-04")
        s.add_argument("--out", default="out")
    sub.choices["extract"].add_argument("--hosts", required=True, help="1行1ホスト名のテキスト")
    sub.choices["extract"].add_argument("--head", type=int, default=1500, help="保存する本文の先頭文字数")
    sub.choices["extract"].add_argument("--limit", type=int, default=0, help="動作確認用の件数上限")
    a = p.parse_args()
    audit(a) if a.cmd == "audit" else extract(a)
