import pandas as pd
from dfsql import sql_query

df = pd.DataFrame({
    "animal": ["cat", "dog", "cat", "dog"],
    "height": [23,  100, 25, 71]
})
df.head()
print(sql_query("SELECT (1 + 2)>2 from animals_df", animals_df=df))