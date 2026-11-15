#!/usr/bin/env python3
r"""Infini-News (ruggsea/infini-news-corpus v2) から日本語ニュースを取得する。
同じフォルダに progress_common.py が必要。

  uv add duckdb
  $env:HF_TOKEN="hf_..."
  # 1) カバレッジ監査: 本文列を読まず、ホスト別の件数・公開日欠損・重複を数える
  uv run python infini_fetch.py audit --months 2024-01
  # 2) 本抽出: 対象ホストの全記事を、本文の先頭だけ残して保存する
  uv run python infini_fetch.py extract --months 2024-01 --hosts hosts.txt --limit 2000   # 動作確認
  uv run python infini_fetch.py extract --months 2016-08:2026-04 --hosts hosts.txt        # 全期間

進捗: 画面に「[何件目/全体] 月 結果 秒数 | 経過 | 残り約」を出す。同じ内容を out/progress.log にも保存する。
      別のPowerShellで  Get-Content out\progress.log -Wait  を実行すると、途中経過を見続けられる。

注意:
- パーティションは「クロール月」であり、記事の公開月ではない。公開日は publish_date 列（欠損あり）。
- 重複（再クロール・転載）は残っている。抽出時は分母を保つため、キーワードでは絞らない。
- 未実行のコードです。止まっても、同じコマンドを再実行すれば済んだ月を飛ばして続きから進む。
"""
import argparse
import os
import time

import duckdb

from progress_common import Tracker, log, run_sql, set_log

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
        log("HF_TOKEN を使用します")
    else:
        log("警告: HF_TOKEN が未設定です。レート制限に当たりやすくなります")
    return con


def audit(args):
    os.makedirs(args.out, exist_ok=True)
    months = parse_months(args.months)
    log(f"audit 開始: {len(months)}か月分")
    con = connect()
    tr = Tracker(len(months))
    for y, m in months:
        label = f"{y}-{m:02d}"
        out = os.path.join(args.out, f"audit_{label}.csv")
        if os.path.exists(out):
            tr.skipped(label)
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
        r = run_sql(con, sql, label)
        tr.done(label, r, time.time() - t)


def extract(args):
    os.makedirs(args.out, exist_ok=True)
    with open(args.hosts, encoding="utf-8") as f:
        hosts = [h.strip() for h in f if h.strip() and not h.startswith("#")]
    cond = " OR ".join(f"url_hostname = '{h}' OR url_hostname LIKE '%.{h}'" for h in hosts)
    lim = f"LIMIT {int(args.limit)}" if args.limit else ""
    months = parse_months(args.months)
    log(f"extract 開始: {len(months)}か月分、対象ホスト {len(hosts)}件")
    con = connect()
    tr = Tracker(len(months))
    for y, m in months:
        label = f"{y}-{m:02d}"
        out = os.path.join(args.out, f"news_{label}.parquet")
        if os.path.exists(out):
            tr.skipped(label)
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
        r = run_sql(con, sql, label)
        if r == "ok":
            os.replace(tmp, out)
            r = f"ok {os.path.getsize(out) / 1e6:.1f}MB"
        tr.done(label, r, time.time() - t)


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
    set_log(os.path.join(a.out, "progress.log"))
    audit(a) if a.cmd == "audit" else extract(a)
