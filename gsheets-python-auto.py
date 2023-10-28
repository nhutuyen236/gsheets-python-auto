import psycopg2
import pandas as pd
from decimal import Decimal
import gspread
import numpy as np

# Connecting to the Google Sheets

# Defining the Service Account from local
gc = gspread.service_account("aaa") # Change the Path accordingly

# Open Spreadsheet by url
sh = gc.open_by_key("aaa")

# Defining the tabs accordingly
ds_status_ws = sh.worksheet("ds_status") 
po_data_ws = sh.worksheet("po_data")
sku_po_ws =sh.worksheet("sku_po")
sku_receipt_ws = sh.worksheet("sku_receipt")
po_receipt_ws = sh.worksheet("po_receipt")
inventory_ws = sh.worksheet("inventory")
do_tracking_ws = sh.worksheet("do_tracking")

# Function to clear a worksheet
def clear_worksheet(worksheet):
    worksheet.clear()

# Clear each worksheet before updating the data
clear_worksheet(ds_status_ws)
clear_worksheet(po_data_ws)
clear_worksheet(sku_po_ws)
clear_worksheet(sku_receipt_ws)
clear_worksheet(po_receipt_ws)
clear_worksheet(inventory_ws)
clear_worksheet(do_tracking_ws)

# PostgreSQL connection details
# input your credentials to connect to db
db_host  = '###'
db_port  = '###'
db_name  = '###'
db_user  = '###'
db_password  = '###'

print("Credentials saved...")

 # Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )

print("Connected to the database")

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# 1 - PO_DATA QUERY

with open('path_of_sql_file', 'r') as file:
    query_po_data = file.read()

cursor.execute(query_po_data)
results_po_data = cursor.fetchall()

columns_headers_po_data = [desc[0] for desc in cursor.description]

# Convert Decimal objects to float or string
results_po_data = [[float(item) if isinstance(item, Decimal) else item for item in row] for row in results_po_data]

# Create a DataFrame from the query results
po_data_df = pd.DataFrame(results_po_data, columns=columns_headers_po_data)

#Convert the Timestamp column to string representation
po_data_df['confirmed_date'] = pd.to_datetime(po_data_df['confirmed_date'])

# Convert datetime values to the desired date format
po_data_df['confirmed_date'] = po_data_df['confirmed_date'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Convert NaN values to empty strings
po_data_df = po_data_df.fillna('')

# Convert DataFrame to a list of lists
po_data_values = [po_data_df.columns.values.tolist()] + po_data_df.values.tolist()

# Update the worksheet with the modified data
po_data_ws.update(po_data_values)

po_data_ws.update([po_data_df.columns.values.tolist()] + po_data_df.values.tolist())

# 2 - INVENTORY QUERY 

with open('path_of_sql_file', 'r') as file:
    query_inventory = file.read()

cursor.execute(query_inventory)

result_inventory = cursor.fetchall()

columns_headers_inventory = [desc[0] for desc in cursor.description]

# Convert Decimal objects to float or string
results_inventory = [[float(item) if isinstance(item, Decimal) else item for item in row] for row in result_inventory]

# Create a DataFrame from the query results
inventory_df = pd.DataFrame(results_inventory, columns=columns_headers_inventory)
inventory_ws.update([inventory_df.columns.values.tolist()] + inventory_df.values.tolist())

# 3 - PO_RECEIPT QUERY 
with open('path_of_sql_file', 'r') as file:
    PO_RECEIPT_query = file.read()

cursor.execute(PO_RECEIPT_query)
po_receipt_results = cursor.fetchall()

po_receipt_columns_headers = [desc[0] for desc in cursor.description]

# Convert Decimal objects to float or string
po_receipt_results= [[str(item) if isinstance(item, (Decimal, int, float)) else item for item in row] for row in po_receipt_results]

# Create a DataFrame from the query results
po_receipt_df = pd.DataFrame(po_receipt_results, columns=po_receipt_columns_headers)

po_receipt_df_metrics = ['demand', 'done', 'missing_qty']
for column in po_receipt_df_metrics:
    
    po_receipt_df[column] = po_receipt_df[column].astype(float)
    po_receipt_df[column].replace({np.nan: ''}, inplace=True)

po_receipt_ws.update([po_receipt_df.columns.values.tolist()] + po_receipt_df.values.tolist())

# 4 - SKU_PO QUERY 

with open('path_of_sql_file', 'r') as file:
    query_sku_po = file.read()

cursor.execute(query_sku_po)

result_sku_po = cursor.fetchall()

columns_headers_sku_po = [desc[0] for desc in cursor.description]

# Convert Decimal objects to float or string
results_sku_po = [[float(item) if isinstance(item, Decimal) else item for item in row] for row in result_sku_po]

# Create a DataFrame from the query results
sku_po_df = pd.DataFrame(results_sku_po, columns=columns_headers_sku_po)
sku_po_ws.update([sku_po_df.columns.values.tolist()] + sku_po_df.values.tolist())

#5 - SKU_RECEIPT QUERY
with open('path_of_sql_file', 'r') as file:
    query_sku_receipt = file.read()

cursor.execute(query_sku_receipt)
sku_receipt_results = cursor.fetchall()

sku_receipt_columns_headers = [desc[0] for desc in cursor.description]

# Convert Decimal objects to float or string
sku_receipt_results = [[float(item) if isinstance(item, (Decimal, int, float)) else item for item in row] for row in sku_receipt_results]

# Create a DataFrame from the query results
sku_receipt_df = pd.DataFrame(sku_receipt_results, columns=sku_receipt_columns_headers)

sku_receipt_df_metrics = ['quantity','qty_received']
for column in sku_receipt_df_metrics:
    
    sku_receipt_df[column] = sku_receipt_df[column].astype(float)
    sku_receipt_df[column].replace({np.nan: ''}, inplace=True)


sku_receipt_ws.update([sku_receipt_df.columns.values.tolist()] + sku_receipt_df.values.tolist())


#6 - DS_STATUS QUERY
with open('path_of_sql_file', 'r') as file:
    query_ds_status = file.read()

cursor.execute(query_ds_status)

result_ds_status = cursor.fetchall()

columns_headers_ds_status = [desc[0] for desc in cursor.description]

# Convert Decimal objects to float or string
results_ds_status = [[float(item) if isinstance(item, Decimal) else item for item in row] for row in result_ds_status]

# Create a DataFrame from the query results
ds_status_df = pd.DataFrame(results_ds_status, columns=columns_headers_ds_status)
ds_status_ws.update([ds_status_df.columns.values.tolist()] + ds_status_df.values.tolist())


#7 - DO_TRACKING QUERY
with open('path_of_sql_file', 'r') as file:
    query_do_tracking = file.read()

cursor.execute(query_do_tracking)
results_do_tracking = cursor.fetchall()

columns_headers_do_tracking = [desc[0] for desc in cursor.description]

# Convert Decimal objects to float or string
results_do_tracking = [[float(item) if isinstance(item, Decimal) else item for item in row] for row in results_do_tracking]

# Create a DataFrame from the query results
do_tracking_df = pd.DataFrame(results_do_tracking, columns=columns_headers_do_tracking)

#Convert the Timestamp column to string representation
do_tracking_df['created_on'] = pd.to_datetime(do_tracking_df['created_on'])

# Convert datetime values to the desired date format
do_tracking_df['created_on'] = do_tracking_df['created_on'].dt.strftime('%Y-%m-%d %H:%M:%S')

# Convert NaN values to empty strings
do_tracking_df = do_tracking_df.fillna('')

# Convert DataFrame to a list of lists
do_tracking_values = [do_tracking_df.columns.values.tolist()] + do_tracking_df.values.tolist()

# Update the worksheet with the modified data
do_tracking_ws.update(do_tracking_values)

do_tracking_ws.update([do_tracking_df.columns.values.tolist()] + do_tracking_df.values.tolist())
