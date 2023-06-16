import pandas as pd
from google.cloud import bigquery
import streamlit as st
import plotly.express as px
from streamlit_autorefresh import st_autorefresh
import json

st_autorefresh(interval=60000, key="asdfadsf")

stock_symbol= st.selectbox("Select stock",["AAPL","NVDA","AMZN","MSFT"])

#Set up Google Cloud authentication (replace "path/to/service-account.json" with your service account JSON file path)
client = bigquery.Client.from_service_account_json("service_key.json")
# client = bigquery.Client('dataengg2')

# Specify the project ID, dataset ID, and table ID for basic finace data
project_id = 'dataengg2'
dataset_id = 'De2Stocks'
table_id = 'basic_finance_data'

# Construct the BigQuery table reference
table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

# Query the data from BigQuery
query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
df_fd = client.query(query).to_dataframe()


# Display the KPI card
st.title(stock_symbol)



st.metric(label="Net Profit Margin Annual", value=f"${df_fd[df_fd['symbol']==stock_symbol]['netProfitMarginAnnual'].values[0]}M")

st.metric(label="Roi Annual", value=f"${df_fd[df_fd['symbol']==stock_symbol]['roiAnnual'].values[0]}M")


st.metric(label="52 Week High", value=f"${df_fd[df_fd['symbol']==stock_symbol]['52WeekHigh'].values[0]}M")

st.metric(label="52 Week Low", value=f"${df_fd[df_fd['symbol']==stock_symbol]['52WeekLow'].values[0]}M")

st.metric(label="eps Growth 3Y", value=f"${df_fd[df_fd['symbol']==stock_symbol]['epsGrowth3Y'].values[0]}M")







# Set the news headlines

project_id = 'dataengg2'
dataset_id = 'De2Stocks'
table_id = 'news_data'

# # Construct the BigQuery table reference
table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

# # Query the data from BigQuery
query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
df_news = client.query(query).to_dataframe()



news3=  df_news[df_news['symbol']==stock_symbol]['summary']
news2 = pd.DataFrame(news3).reset_index(drop=True)
print(news2)

news_headlines2 = news2.loc[0]['summary'] 



# # Display the scrolling news headlines with margins
st.title("News")
st.markdown(
    """
    <style>
        .scrolling-container {
            width: 100%;
            white-space: nowrap;
            overflow: hidden;
            padding: 10px;  /* Adjust the padding as per your preference */
            margin: 10px;  /* Adjust the margin as per your preference */
        }

        .scrolling-text {
            display: inline-block;
            animation: marquee 20s linear infinite;
            font-size: 30px;
        }

        @keyframes marquee {
            0% { transform: translateX(100%); }
            100% { transform: translateX(-100%); }
        }
    </style>
    """,
    unsafe_allow_html=True
)


st.markdown(f'<div class="scrolling-container"><div class="scrolling-text">{news_headlines2}</div></div>', unsafe_allow_html=True)







# Specify the project ID, dataset ID, and table ID
project_id = 'dataengg2'
dataset_id = 'De2Stocks'
table_id = 'stocks_data'

# Construct the BigQuery table reference
table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

# Query the data from BigQuery
query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` "
df_all = client.query(query).to_dataframe()





# Get unique stock symbols


filtered_df = df_all[df_all['symbol'] == stock_symbol]
    # Create dynamic variable names (e.g., df1, df2, df3, ...)
   



df_all = filtered_df.sort_values(by='datetime', ascending=False)

#print(dfAAPL)
# print(dfAMZN)
# print(dfGOOG)
# print(dfNVDA)
# print(dfMSFT)





# Create tabs
tabs = ["1 Day", "5 Day", "30 Day"]
selected_tab = st.sidebar.radio("Select your option", tabs)



# Filter data for recent 1 day
recent_1day = df_all[df_all['datetime'] >= df_all['datetime'].max() - pd.DateOffset(days=1)]

# Filter data for recent 5 days
recent_5days = df_all[df_all['datetime'] >= df_all['datetime'].max() - pd.DateOffset(days=5)]

# Filter data for recent 1 month
recent_1month = df_all[df_all['datetime'] >= df_all['datetime'].max() - pd.DateOffset(months=1)]





# Filter the data based on selected tab
if selected_tab == "1 Day":
    # filtered_data = df_all[df_all['datetime'].notna() & df_all['datetime'].str.contains('2023-06-07')]
    filtered_data = recent_1day

elif selected_tab == "5 Day":
    #filtered_data = df_all.tail(5)
    filtered_data = recent_5days
elif selected_tab == "30 Day":
    #filtered_data = df_all.tail(30)
    filtered_data= recent_1month


# Create the plot
fig = px.line(filtered_data, x='datetime', y='close', color='symbol')

# Display the plot
st.plotly_chart(fig)
