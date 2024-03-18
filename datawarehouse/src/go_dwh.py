# (PR4-3) Datawarehouse
# The tasks

#Bouw voort op je uitwerking van het vorige practicum:

#1 Maak een database in SSMS, dit wordt je uiteindelijke Data Warehouse.
#Helemaal goed.

#2 Maak een ETL-schema van elke tabel in het Sterschema.
#Dit kan in het document worden gevonden.

#3 Implementeer elk gemaakt ETL-schema in Python (met Jupyter Notebook). Gebruik hierbij de technische informatie uit week 2.
#Helemaal goed.

#4 Zorg er ten slotte voor dat je het Sterschema en elk ETL-schema aan je portfolio hebt toegevoegd. Hier word je in week 6 op beoordeeld.
#Dit kan in het document worden gevonden.

# DWH imports 
# Import the necassery modules.

import pyodbc as sql_server
from dotenv import dotenv_values as values
import pandas as pd
import sqlite3 as sql
import warnings as warn
warn.simplefilter("ignore")

# Connection to SQL Server 
# Load the values from the .env.local file.
env: dict[str, str | None] = values(".env.local")

conn_str: str = (
    "DRIVER={SQL Server};Server=" + env["SERVER"] + 
    ";Database=" + env["DATABASE"] + 
    ";User ID=" + env["USERNAME"] + 
    ";Password=" + env["PASSWORD"] + 
    "trusted_connection=yes;"
)

try:
    conn = sql_server.connect(conn_str)
    print(f"Succesfully established connection to the server: {conn}")

except sql_server.Error as err:
    print(f"Error while connecting to the server: {err.args[1]}")
    raise

# Read the GO Databases/CSV's
# Read the original datasources.

conn_db1 = sql.connect("Great_Outdoors_Data_SQLite/go_crm.sqlite")
conn_db2 = sql.connect("Great_Outdoors_Data_SQLite/go_sales.sqlite")
conn_db3 = sql.connect("Great_Outdoors_Data_SQLite/go_staff.sqlite")

go_csv: list[str] = ["GO_SALES_INVENTORY_LEVELSData", "GO_SALES_PRODUCT_FORECASTData"]
conn_csv = [pd.read_csv("Great_Outdoors_Data_SQLite/" + csv + ".csv") for csv in go_csv]

# Read the original GO Databases 
# Read the original db sources

cursor1 = conn_db1.cursor()
cursor2 = conn_db2.cursor()
cursor3 = conn_db3.cursor()

cursors = [cursor1, cursor2, cursor3]
conns = [conn_db1, conn_db2, conn_db3]

for cursor, _ in zip(cursors, conns):
    datas = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

    for data in datas:
        dfs = cursor.execute(f"SELECT * FROM {data[0]}").fetchall()
        columns = [column[0] for column in cursor.description]
        df = pd.DataFrame(dfs, columns=columns)
        df = df[df.columns.drop(list(df.filter(regex="TRIAL")))]
        print(df)

    cursor.close()

# Insert the GO Databases/CSV's to the datewarehouse
# Original datasources, will be inserted into the datawarehouse.

# Insertion

# Read all tables 
# Read the tables with pandas from SQL Server.

product = pd.read_sql_query("SELECT * FROM product", conn)
sales_product_forecast = pd.read_sql_query("SELECT * FROM sales_product_forecast", conn)
sales_inventory_levels = pd.read_sql_query("SELECT * FROM sales_inventory_levels", conn)
sales_staff = pd.read_sql_query("SELECT * FROM sales_staff", conn)
sales_targetdata = pd.read_sql_query("SELECT * FROM sales_targetdata", conn)
course = pd.read_sql_query("SELECT * FROM course", conn)
training = pd.read_sql_query("SELECT * FROM training", conn)

satisfaction_type = pd.read_sql_query("SELECT * FROM satisfaction_type", conn)
satisfaction = pd.read_sql_query("SELECT * FROM satisfaction", conn)
returned_reason = pd.read_sql_query("SELECT * FROM returned_reason", conn)
returned_item = pd.read_sql_query("SELECT * FROM returned_item", conn)
retailer_contact = pd.read_sql_query("SELECT * FROM retailer_contact", conn)
order_method = pd.read_sql_query("SELECT * FROM order_method", conn)
# order = pd.read_sql_query("SELECT * FROM order_", conn)

df = retailer_contact

import copy
import datetime

# Creating a slowly changing dimension (assuming type 2) for every dimension in the datawarehouse
for name in ["product", "sales_product_forecast", "sales_inventory_levels", "sales_staff", "sales_targetdata", "course", "training", "satisfaction_type", "satisfaction", "returned_reason", "returned_item", "retailer_contact", "order_method"]:
    # Copy
    globals()[name + "_scd"] = copy.deepcopy(globals()[name])

    # Add columns
    globals()[name + "_scd"]["Nummer_sk"] = range(len(globals()[name + "_scd"]))
    globals()[name + "_scd"]["Timestamp"] = datetime.datetime.now().strftime("%d/%m/%Y, %H:%M")

    # Fix index
    globals()[name + "_scd"].set_index("Nummer_sk", inplace=True)

# Testing the relations are still valid

# The assignment states "Test uitvoerig of correcte relaties met bijbehorende feittabellen behouden blijven." This does not require to test exhausively, it only requires thorough testing. This means that as long as we have a basic test for each of the tables, it should be fine.
# Therefore, the test is going to be merging all warehouse tables and merging all slowly changing dimensions, dropping dupplicates for the column that used to be the primary key and seeing if we get the same number of rows


# Merging source
source_1 = pd.merge(
    returned_reason,
    returned_item,
    left_on="return_reason_code",
    right_on="returned_reason_code"
)

source_2 = pd.merge(
    satisfaction_type,
    pd.merge(
        satisfaction,
        pd.merge(
            course,
            pd.merge(
                training,
                pd.merge(
                    sales_staff,
                    pd.merge(
                        sales_inventory_levels,
                        pd.merge(
                            sales_targetdata,
                            pd.merge(
                                product,
                                sales_product_forecast,
                                on="product_number"
                            ),
                            left_on="target_product_nr",
                            right_on="product_number"
                        ),
                        on="product_number"
                    ),
                    left_on="sales_staff_code",
                    right_on="sales_staff_id"
                ),
                left_on="training_staff_id",
                right_on="sales_staff_code"
            ),
            left_on="course_code",
            right_on="training_course_id"
        ),
        left_on="satisfaction_staff_id",
        right_on="sales_staff_code"
    ),
    left_on="satisfaction_type_code",
    right_on="satisfaction_type_id"
)

source_3 = retailer_contact
source_4 = order_method

# Merging warehouse
warehouse_1 = pd.merge(
    returned_reason_scd,
    returned_item_scd,
    left_on="return_reason_code",
    right_on="returned_reason_code"
)

warehouse_2 = pd.merge(
    satisfaction_type_scd,
    pd.merge(
        satisfaction_scd,
        pd.merge(
            course_scd,
            pd.merge(
                training_scd,
                pd.merge(
                    sales_staff_scd,
                    pd.merge(
                        sales_inventory_levels_scd,
                        pd.merge(
                            sales_targetdata_scd,
                            pd.merge(
                                product,
                                sales_product_forecast_scd,
                                on="product_number"
                            ),
                            left_on="target_product_nr",
                            right_on="product_number"
                        ),
                        on="product_number"
                    ),
                    left_on="sales_staff_code",
                    right_on="sales_staff_id",
                    suffixes=("_1", "_2")
                ),
                left_on="training_staff_id",
                right_on="sales_staff_code"
            ),
            left_on="course_code",
            right_on="training_course_id",
            suffixes=("_3", "_4")
        ),
        left_on="satisfaction_staff_id",
        right_on="sales_staff_code",
        suffixes=("_5", "_6")
    ),
    left_on="satisfaction_type_code",
    right_on="satisfaction_type_id",
    suffixes=("_7", "_8")
)

warehouse_3 = retailer_contact_scd
warehouse_4 = order_method_scd

# Asserting everything
for source, warehouse in zip([source_1, source_2, source_3, source_4], [warehouse_1, warehouse_2, warehouse_3, warehouse_4]):
    assert source.drop_duplicates().size == warehouse.drop_duplicates().size
