import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
from sqlalchemy import create_engine
import scipy.stats as stats
warnings.filterwarnings('ignore')


# Create database connection
connection_string = "mysql+mysqlconnector://root:root@localhost/inventory"
engine = create_engine(connection_string)

#Fetching vendor summary data
df = pd.read_sql_query("select * from vendor_sales_summary", engine)
#print(df.head())

#print(df.describe().T)

# Distribution Plots For Numerical Columns
numerical_cols = df.select_dtypes(include=np.number).columns

plt.figure(figsize=(15,10))
for i, col in enumerate(numerical_cols):
    plt.subplot(4,4,i+1) # Adjust grid layout needed
    sns.histplot(df[col], kde=True, bins=30)
    plt.title(col)
plt.tight_layout()
#plt.show()


# Outlier Detection with Boxplots
plt.figure(figsize=(15,10))
for i, col in enumerate(numerical_cols):
    plt.subplot(4,4,i+1) # Adjust grid layout needed
    sns.boxplot(y=df[col])
    plt.title(col)
plt.tight_layout()
#plt.show()


# lets filter the data by removing inconsistencies
df = pd.read_sql_query("""SELECT * FROM vendor_sales_summary WHERE GrossProfit > 0 
                       AND ProfitMargin > 0 AND TotalSalesQuantity > 0""", engine)
print(df.head())