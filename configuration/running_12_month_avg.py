import pandas as pd
from collections import deque
# relies too much on the order from the monthly_rates2.csv, which, in turn, relies on the http response from the XE API. tested and appears to have same values as the one previously used.


# Create a deque with a maximum length of 12
cache_currency = deque(maxlen=12)
cache_avg_and_days = deque(maxlen=12)
df = pd.read_csv("monthly_rates.csv", header=0)
last_12_cur = set()
print(f"from,end_of_year,total_days,rolling_12_months_avg_rate")
for _,r in df.iterrows():
    cache_currency.append(r['from'])
    cache_avg_and_days.append((r['average_rate'],r['number_of_days']))
    if(len(cache_currency) == 12 and len(set(cache_currency)) == 1):
        total_days = 0
        x_rate_sum = 0
        for x_rate, ndays in cache_avg_and_days:
            x_rate_sum += x_rate * ndays
            total_days += ndays
        avg_rate = x_rate_sum/total_days
        print(f"{r['from']},{r['end_of_month']},{total_days},{avg_rate}")