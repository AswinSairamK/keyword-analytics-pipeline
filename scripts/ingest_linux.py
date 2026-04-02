import os
import time
import pandas as pd
from pytrends.request import TrendReq

# Linux path to Windows folder
BASE = "/mnt/c/Users/Admin/Documents/Project/Python Project/ETL Project/Digital_Marketing_Keyword"

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

pytrends = TrendReq(hl="en-US", tz=360)

print(f"\nFetching trends for: {KEYWORDS}\n")

pytrends.build_payload(
    KEYWORDS,
    cat=0,
    timeframe="today 3-m",
    geo="",
    gprop=""
)

df_trend = pytrends.interest_over_time()

if df_trend.empty:
    print("No data returned")
else:
    df_trend = df_trend.drop(columns=["isPartial"], errors="ignore")
    df_trend = df_trend.reset_index()
    df_trend.rename(columns={"date": "week"}, inplace=True)
    print(f"Rows fetched: {len(df_trend)}")
    out_path = os.path.join(BASE, "raw", "keyword_trends.csv")
    df_trend.to_csv(out_path, index=False)
    print(f"Saved to: {out_path}")

time.sleep(2)
related = pytrends.related_queries()
all_related = []


print("\nFetching related queries...")
time.sleep(10)  

try:
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
    else:
        print("No related queries returned")

except Exception as e:
    print(f"Related queries skipped — rate limited: {e}")
    print("Main trend data already saved — pipeline continues!")

print("\nIngest complete!")
