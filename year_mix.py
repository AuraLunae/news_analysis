r"""out/local/topics_by_year.csv から、年ごとのジャンル構成比（%）を表示する。
  uv run python year_mix.py
"""
import duckdb

TOPICS = ["economy_business_finance", "arts_culture", "sport", "human_interest",
          "politics", "lifestyle", "crime_law", "society"]
cols = ",\n  ".join(
    f"round(100.0 * sum(n) FILTER (WHERE iptc_topic = '{t}') / sum(n), 1) AS {t}" for t in TOPICS
)
sql = f"""
SELECT year, sum(n) AS n_jpn,
  {cols}
FROM read_csv('out/local/topics_by_year.csv')
GROUP BY year ORDER BY year
"""
rel = duckdb.sql(sql)
names = rel.columns
short = {"economy_business_finance": "economy", "human_interest": "human_int"}
print("  ".join(f"{short.get(c, c):>10}" for c in names))
for row in rel.fetchall():
    print("  ".join(f"{(v if v is not None else 0):>10,}" if i == 1 else f"{(v if v is not None else 0):>10}"
                    for i, v in enumerate(row)))
