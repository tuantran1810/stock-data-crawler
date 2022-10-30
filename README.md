# stock-data-crawler
This is the crawler and processor for acquiring information of Vietnamese stock market.

### Running order
- 1_crawl.py: for acquiring daily pricing data and quarterly financial report.
- 2_trends.py: for acquiring interest over time ratio from google trends.
- 3_events.py: for acquiring volume change events.
- 4_est_shares.py: for calculating shares on the market using owner's equity and bvps.
- 5_market_cap_quarterly.py: for calculating market cap using the est shares. Data is correct manually since it's not perfect.
- 6_amihud.py: for calculating amihud.
