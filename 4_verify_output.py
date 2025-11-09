# 4_verify_output.py
import pandas as pd
import matplotlib.pyplot as plt

# Load merged dataset
df = pd.read_csv("Merged_Dataset.csv", parse_dates=["Date"])
print("\nBasic Info:")
print(df.info())
print("\nSummary Stats:")
print(df.describe())

# --- Plot Time Series ---
plt.figure(figsize=(10,5))
plt.plot(df["Date"], df["O3_ug_m3"], label="Ozone (µg/m³)")
plt.title("Monthly Average Ozone Concentration (EPA)")
plt.xlabel("Year")
plt.ylabel("O3 (µg/m³)")
plt.grid(True)
plt.legend()
plt.show()

plt.figure(figsize=(10,5))
plt.plot(df["Date"], df["T2M"], color='orange', label="Temperature (°C)")
plt.title("Monthly Mean Temperature (NASA POWER)")
plt.xlabel("Year")
plt.ylabel("Temperature (°C)")
plt.grid(True)
plt.legend()
plt.show()

plt.figure(figsize=(10,5))
plt.plot(df["Date"], df["PRECTOTCORR"], color='blue', label="Precipitation (mm/month)")
plt.title("Monthly Total Precipitation (NASA POWER)")
plt.xlabel("Year")
plt.ylabel("Precipitation (mm)")
plt.grid(True)
plt.legend()
plt.show()

# --- Correlation Matrix ---
corr = df[['O3_ug_m3', 'T2M', 'PRECTOTCORR']].corr()
print("\nCorrelation Matrix:\n", corr)
