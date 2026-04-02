import os
import time
import pandas as pd
from pytrends.request import TrendReq
from datetime import datetime

BASE = r"C:\Users\Admin\Documents\Project\Python Project\ETL Project\Digital_Marketing_Keyword"

# ── Keywords to track ────────────────────────────────────────────
# These are digital marketing related keywords
KEYWORDS = [
    "SEO",
    "digital marketing",
    "social media marketing",
    "content marketing",
    "email marketing"
]

print("=" * 55)
print("KEYWORD INGEST — Google Trends")
print("=" * 55)

# ── Connect to Google Trends ─────────────────────────────────────
pytrends = TrendReq(hl="en-US", tz=360)

# ── Fetch interest over time (last 90 days) ───────────────────────
print(f"\nFetching trends for: {KEYWORDS}\n")

pytrends.build_payload(
    KEYWORDS,
    cat=0,
    timeframe="today 3-m",   # last 90 days
    geo="",                   # worldwide
    gprop=""
)

# ── Get interest over time ────────────────────────────────────────
df_trend = pytrends.interest_over_time()

if df_trend.empty:
    print("No data returned — try again in a few minutes")
else:
    # Drop the isPartial column
    df_trend = df_trend.drop(columns=["isPartial"], errors="ignore")
    df_trend = df_trend.reset_index()
    df_trend.rename(columns={"date": "week"}, inplace=True)

    print(f"Rows fetched   : {len(df_trend)}")
    print(f"Columns        : {list(df_trend.columns)}")
    print(f"\nSample data:")
    print(df_trend.head(5).to_string(index=False))

    # Save to raw folder
    out_path = os.path.join(BASE, "raw", "keyword_trends.csv")
    df_trend.to_csv(out_path, index=False)
    print(f"\nSaved to: {out_path}")

# ── Fetch related queries ─────────────────────────────────────────
print("\nFetching related queries...")
time.sleep(2)   # avoid rate limiting

related = pytrends.related_queries()
all_related = []

for kw in KEYWORDS:
    try:
        top = related[kw]["top"]
        if top is not None and not top.empty:
            top["source_keyword"] = kw
            all_related.append(top)
    except Exception:
        pass

if all_related:
    df_related = pd.concat(all_related, ignore_index=True)
    related_path = os.path.join(BASE, "raw", "related_queries.csv")
    df_related.to_csv(related_path, index=False)
    print(f"Related queries saved: {len(df_related)} rows")
    print(df_related.head(5).to_string(index=False))

print("\nIngest complete!")