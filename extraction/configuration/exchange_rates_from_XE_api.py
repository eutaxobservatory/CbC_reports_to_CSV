import requests
import json

# using free  for XE API. New credentials will be needed!
base_currency = "EUR"
currencies_of_interest = ["EUR",
                          "NOK",
                          "MXN",
                          "USD",
                          "ZAR",
                          "GBP",
                          "INR",
                          "DKK",
                          "COP",
                          "JPY",
                          "SEK",
                          "AUD",
                          "CHF",
                          "BRL",
                          "PLN",
                          "CNY",
                          "CAD",
                          "HKD",
                          "SGD",
                          "THB",
                          "MYR",
                          "NZD",
                          "RUB",
                          "TWD"
                          ]
base = 'https://xecdapi.xe.com/v1/monthly_average/'
print("from,to,end_of_month,average_rate,number_of_days")

# end_march = f"{year}.03.31"
# end_june = f"{year}.06.30"
# end_december = f"{year}.12.31"
    # print(f"EUR,EUR,{year},1")
for cur in currencies_of_interest:
    for year in range(2010, 2023):
        msg = f"{base}?from={cur}&to={base_currency}&year={year}"
        response = requests.get(
            msg,
            auth=('vaitefoder773176967', 'uumk0r6ia8k1ek8augb2uun5m1') # using free trial, new credentials needed!
        )

        data = json.loads(response.content)
        # print(data)
        jsonData = data["to"]['EUR']
        out = 0
        days_year = 0
        for month in jsonData:
            avg = month["monthlyAverage"]
            days = month["daysInMonth"]
            print(f"{cur},{base_currency},{year}.{str(month['month']).rjust(2, '0')}.{days},{avg},{days}")
        #     days_year += days
        #     out += avg * days
        # if days_year > 366 or days_year < 364:
        #     raise ValueError("Wrong number of days.")
        # out = out / days_year
        # print(f"{cur},{base_currency},{year}.{month},{out}")