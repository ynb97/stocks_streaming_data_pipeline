import pandas as pd
from google.cloud import bigquery

df = pd.read_csv('AAPL_mar-june.csv')
df['datetime'] = pd.to_datetime(df['datetime'],format='%d-%m-%Y %H:%M')

client = bigquery.Client('dataengg2')

job_config = bigquery.LoadJobConfig(

    # Specify a (partial) schema. All columns are always written to the

    # table. The schema is used to assist in data type definitions.

    schema=[

        # Specify the type of columns whose type cannot be auto-detected. For

        # example the "title" column uses pandas dtype "object", so its

        # data type is ambiguous.

        bigquery.SchemaField("symbol", bigquery.enums.SqlTypeNames.STRING),

        # Indexes are written if included in the schema by name.

        bigquery.SchemaField("datetime", bigquery.enums.SqlTypeNames.TIMESTAMP),

        bigquery.SchemaField("open", bigquery.enums.SqlTypeNames.FLOAT64),

        bigquery.SchemaField("high", bigquery.enums.SqlTypeNames.FLOAT64),

        bigquery.SchemaField("low", bigquery.enums.SqlTypeNames.FLOAT64),

        bigquery.SchemaField("close", bigquery.enums.SqlTypeNames.FLOAT64),

        bigquery.SchemaField("volume", bigquery.enums.SqlTypeNames.FLOAT64)

    ],

    # Optionally, set the write disposition. BigQuery appends loaded rows

    # to an existing table by default, but with WRITE_TRUNCATE write

    # disposition it replaces the table with the loaded data.

    write_disposition="WRITE_TRUNCATE",

)

table_id = "dataengg2.De2Stocks.stocks_data"

 

job = client.load_table_from_dataframe(

    df, table_id, job_config=job_config

)  # Make an API request.

job.result()  # Wait for the job to complete.

 

table = client.get_table(table_id)  # Make an API request.

print(

    "Loaded {} rows and {} columns to {}".format(

        table.num_rows, len(table.schema), table_id

    )

)