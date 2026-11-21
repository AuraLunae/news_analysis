r"""Fine-News の research/coverage.parquet（約10MB）から、日本の媒体（ドメイン）ごとの総件数を出す。
記事本文は読まない。表は縦持ち（stage / dimension / value）で、dimension は
  domain（媒体ごとの合計）/ language / month（月ごとの合計）/ all
の4種類。媒体×月、媒体×言語の組み合わせは入っていない。つまり「各媒体が全期間で何件か」は分かるが、
「いつからいつまで収録されているか」は分からない（それは audit で月を絞って調べる）。

  uv run python coverage_jp.py inspect
  uv run python coverage_jp.py jp                    # .jp ドメイン＋ hosts.txt の媒体を一覧
  uv run python coverage_jp.py jp --top 100 --hosts hosts.txt

stage の意味: source=Infini-News全体(1,372,589,195件、重複あり) / stage1=前処理後 /
  stage2=重複ファミリーの代表のみ / stage3=品質フィルタ通過 / stage4=重み付け後。
  → pct_after_dedup が低い媒体は、他の記事と重複・類似する割合が高い（転載の目安。ただし代表は
    撮影日時が最も早い記事が選ばれるため、重複側の媒体に偏って低く出る）。
出力: out/local/jp_domains.csv

注意: 未実行のコードです。言語での絞り込みはできないため、.jp ドメインと hosts.txt に書いた媒体だけを対象にする
（.com など .jp 以外の日本語媒体は hosts.txt に追記しないと出ない）。
"""
import argparse
import os
import urllib.request

import duckdb

URL = "https://huggingface.co/datasets/lambfool15/fine-news/resolve/main/research/coverage.parquet"
LOCAL = "out/local/coverage.parquet"


def download():
    if os.path.exists(LOCAL):
        return
    os.makedirs(os.path.dirname(LOCAL), exist_ok=True)
    req = urllib.request.Request(URL)
    tok = os.environ.get("HF_TOKEN")
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    print("coverage.parquet をダウンロード中（約10MB）…")
    with urllib.request.urlopen(req, timeout=120) as r, open(LOCAL, "wb") as f:
        f.write(r.read())
    print(f"保存: {LOCAL}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["inspect", "jp"])
    ap.add_argument("--top", type=int, default=80)
    ap.add_argument("--hosts", default="hosts.txt")
    a = ap.parse_args()

    download()
    con = duckdb.connect()
    src = f"read_parquet('{LOCAL}')"

    if a.cmd == "inspect":
        print("dimension ごとの行数と、件数が多い値の例（source段階）:")
        for dim, n in con.execute(
            f"SELECT dimension, count(*) FROM {src} WHERE stage = 'source' GROUP BY 1 ORDER BY 2 DESC"
        ).fetchall():
            ex = [r[0] for r in con.execute(
                f"SELECT value FROM {src} WHERE stage = 'source' AND dimension = ? "
                f"ORDER BY unique_documents DESC LIMIT 5", [dim]).fetchall()]
            print(f"  {dim:<12}{n:>9,} 行  例: {ex}")
        return

    hosts = []
    if os.path.exists(a.hosts):
        hosts = [h.strip() for h in open(a.hosts, encoding="utf-8") if h.strip() and not h.startswith("#")]
    cond = " OR ".join(["domain LIKE '%.jp'", "domain = 'jp'"] +
                       [f"domain = '{h}' OR domain LIKE '%.{h}'" for h in hosts])
    sql = f"""
    WITH d AS (
      SELECT value AS domain,
        sum(unique_documents) FILTER (WHERE stage = 'source') AS n_source,
        sum(missing_publication_date_documents) FILTER (WHERE stage = 'source') AS n_nopub,
        sum(unique_documents) FILTER (WHERE stage = 'stage2') AS n_s2,
        sum(unique_documents) FILTER (WHERE stage = 'stage3') AS n_s3
      FROM {src} WHERE dimension = 'domain' GROUP BY value)
    SELECT domain, n_source,
           round(100.0 * n_nopub / n_source, 1) AS pct_nopub,
           round(100.0 * coalesce(n_s2, 0) / n_source, 1) AS pct_after_dedup,
           round(100.0 * coalesce(n_s3, 0) / n_source, 1) AS pct_after_quality
    FROM d WHERE n_source > 0 AND ({cond}) ORDER BY n_source DESC"""
    os.makedirs("out/local", exist_ok=True)
    con.execute(f"COPY ({sql}) TO 'out/local/jp_domains.csv' (HEADER, DELIMITER ',')")
    rows = con.execute(sql).fetchall()
    total = sum(r[1] for r in rows)
    print(f".jp ドメインと hosts.txt の媒体: {len(rows):,} 個、合計 {total:,} 件（source段階、全期間、重複を含む）")
    print(f"\n--- 件数上位 {a.top} ---")
    print(f"{'媒体':<34}{'件数':>12}{'公開日欠損%':>11}{'重複除去後%':>11}{'品質通過%':>10}")
    for d, n, p, s2, s3 in rows[: a.top]:
        print(f"{d:<34}{n:>12,}{p:>11}{s2:>11}{s3:>10}")

    if hosts:
        print(f"\n--- {a.hosts} に書いた媒体 ---")
        for h in hosts:
            hit = [r for r in rows if r[0] == h or r[0].endswith("." + h)]
            if not hit:
                print(f"{h:<30}なし（表記違い、または未収録）")
            else:
                print(f"{h:<30}" + " / ".join(f"{r[0]} {r[1]:,}" for r in hit[:4]))

    print("\n--- 日本語の言語ラベル（source段階、全期間） ---")
    for lang, n in con.execute(
        f"SELECT value, unique_documents FROM {src} "
        f"WHERE stage = 'source' AND dimension = 'language' AND value LIKE 'jpn%' ORDER BY 2 DESC"
    ).fetchall():
        print(f"  {lang:<14}{n:>12,}")
    print(f"出力: out/local/jp_domains.csv（{len(rows):,} 行）")


if __name__ == "__main__":
    main()
