import pandas as pd
import requests
import matplotlib.pyplot as plt
import seaborn as sns
# Define API URL
url = ("https://power.larc.nasa.gov/api/temporal/daily/point?"
       "parameters=T2M,PRECTOTCORR&community=AG&longitude=-80.8&latitude=35.23"
       "&start=20100101&end=20191231&format=CSV")

# Fetch data
response = requests.get(url)
with open("NASA_POWER_Garinger_2010_2019.csv", "wb") as f:
    f.write(response.content)

