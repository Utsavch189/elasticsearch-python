from elastic import es
from elasticsearch import helpers

# documents = [
#     {"company_name": "Alpha Corp", "report_type": "standalone", "net_profit": 12.5, "profit": 22.1, "revenue": 1000},
#     {"company_name": "Alpha Corp", "report_type": "consolidated", "net_profit": 10.0, "profit": 18.2, "revenue": 1500},
#     {"company_name": "Beta Ltd", "report_type": "standalone", "net_profit": 8.9, "profit": 19.0, "revenue": 900},
#     {"company_name": "Beta Ltd", "report_type": "consolidated", "net_profit": 14.2, "profit": 21.3, "revenue": 1100},
#     {"company_name": "Gamma Industries", "report_type": "standalone", "net_profit": 9.5, "profit": 20.5, "revenue": 1200},
#     {"company_name": "Gamma Industries", "report_type": "consolidated", "net_profit": 11.8, "profit": 25.0, "revenue": 1700},
# ]

# actions = [
#     {
#         "_index": "financial_reports",
#         "_source": doc
#     }
#     for doc in documents
# ]

# helpers.bulk(es, actions)

import pandas as pd

# df1 = pd.read_csv('screener/net profit 2 year.csv')
# df2 = pd.read_csv('screener/net profit 3 year.csv')

# merged_df = pd.merge(df1, df2, on='fincode', how='outer') 

# merged_df.to_csv('screener/merged_output.xls', index=False)

df = pd.read_csv('screener/merged_output.xls')
df = df.fillna("") 
df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]

records = df.to_dict(orient='records')

actions = [
    {
        "_index": "financial_reports",
        "_source": doc
    }
    for doc in records
]

helpers.bulk(es, actions)