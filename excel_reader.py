import pandas as pd

# Read the entire Excel file (first sheet by default)
df = pd.read_excel("Book1.xlsx", engine='openpyxl')
df.fillna(method='ffill', inplace=True)
# Print the first few rows
print(df.head())
