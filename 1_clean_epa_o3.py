import pandas as pd
import glob
import os

# Step 1: Load all O₃ files
epa_files = sorted(glob.glob("EPAair_O3_GaringerNC*_raw.csv"))
epa_list = []

for file in epa_files:
    df = pd.read_csv(file)
    df['Year'] = os.path.basename(file)[14:18]  # extract year from filename
    epa_list.append(df)

# Combine all years into one dataframe
epa = pd.concat(epa_list, ignore_index=True)
print("Raw combined shape:", epa.shape)

# Step 2: Check basic info
print(epa.columns)
print(epa.head())

# Step 3: Keep only key columns
# Adjust column names based on your data
epa = epa[['Date Local', 'Arithmetic Mean', 'Units of Measure']]

# Step 4: Convert date column to datetime
epa['Date Local'] = pd.to_datetime(epa['Date Local'], errors='coerce')

# Step 5: Drop any rows with missing O₃ values
epa = epa.dropna(subset=['Arithmetic Mean'])

# Step 6: Convert all units to µg/m³
# If some data is in ppm, use conversion: 1 ppm O3 ≈ 2140 µg/m³
def convert_to_ug_m3(value, unit):
    if isinstance(unit, str) and 'ppm' in unit.lower():
        return value * 2140
    else:
        return value

epa['O3_ug_m3'] = epa.apply(lambda x: convert_to_ug_m3(x['Arithmetic Mean'], x['Units of Measure']), axis=1)

# Step 7: Aggregate daily → monthly
epa_monthly = epa.groupby(epa['Date Local'].dt.to_period('M'))['O3_ug_m3'].mean().reset_index()
epa_monthly['Date'] = epa_monthly['Date Local'].dt.to_timestamp()
epa_monthly.drop(columns=['Date Local'], inplace=True)

epa_monthly.to_csv("Cleaned_EPA_O3_Monthly.csv", index=False)
print("✅ Cleaned EPA data saved as 'Cleaned_EPA_O3_Monthly.csv'")
