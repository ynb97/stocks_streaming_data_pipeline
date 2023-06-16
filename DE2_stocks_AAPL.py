import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
import streamlit as st
import plotly.express as px



#Set up Google Cloud authentication (replace "path/to/service-account.json" with your service account JSON file path)

client = bigquery.Client('dataengg2')


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
st.title("Apple")



st.metric(label="net Profit Margin Annual", value=f"${df_fd[df_fd['symbol']=='AAPL']['netProfitMarginAnnual'].values[0]}M")

st.metric(label="Roi Annual", value=f"${df_fd[df_fd['symbol']=='AAPL']['roiAnnual'].values[0]}M")


st.metric(label="52 Week High", value=f"${df_fd[df_fd['symbol']=='AAPL']['52WeekHigh'].values[0]}M")

st.metric(label="52 Week Low", value=f"${df_fd[df_fd['symbol']=='AAPL']['52WeekLow'].values[0]}M")

st.metric(label="eps Growth 3Y", value=f"${df_fd[df_fd['symbol']=='AAPL']['epsGrowth3Y'].values[0]}M")





#52WeekHigh, 52WeekLow,epsGrowth3Y,marketCapitalization,netProfitMarginAnnual,roiAnnual,totalDebttotalEquityAnnual,totalDebttotalEquityQuarterly









# Set the news headlines

project_id = 'dataengg2'
dataset_id = 'De2Stocks'
table_id = 'news_data'

# # Construct the BigQuery table reference
table_ref = client.dataset(dataset_id, project=project_id).table(table_id)

# # Query the data from BigQuery
query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
df_news = client.query(query).to_dataframe()

print(df_news)

news3=  df_news[df_news['symbol']=='AAPL']['summary']
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





#print(df_all)



# df_all['datetime_new'] = pd.to_datetime(df_all['datetime'])

# print(df_all)

#df_all= df_all.sort_values('symbol')

# #df_all['datetime2'] = pd.to_datetime(df_all['datetime'])
# df_all['Date_only'] = df_all['datetime_new'].dt.date
# # Sort the DataFrame by date in ascending order
# #df_all = df_all.sort_values('Date_only')
# print(df_all['Date_only'])

# print

#print(df_all)


# Get unique stock symbols
symbols = df_all['symbol'].unique()


# Create individual DataFrames
for i, symbol in enumerate(symbols):
    # Filter the DataFrame by symbol
    filtered_df = df_all[df_all['symbol'] == symbol]
    # Create dynamic variable names (e.g., df1, df2, df3, ...)
    variable_name = f"df{symbol}"
    # Assign the filtered DataFrame to the dynamic variable name
    globals()[variable_name] = filtered_df

# Example usage: Print the individual DataFrames
print("see here")

dfAAPL = dfAAPL.sort_values(by='datetime', ascending=False)

print(dfAAPL)
# print(dfAMZN)
# print(dfGOOG)
# print(dfNVDA)
# print(dfMSFT)



df_all= dfAAPL






#print(df_all)

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


