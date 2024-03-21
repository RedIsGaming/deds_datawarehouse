import datetime
import pandas as pd
import matplotlib.pyplot as plt

import go_dwh
import pyodbc
import proglog


order_details = pd.read_sql_query("SELECT * FROM order_details", go_dwh.conns[1])
product = pd.read_sql_query("SELECT * FROM product", go_dwh.conns[1])
retailer = pd.read_sql_query("SELECT * FROM retailer", go_dwh.conns[0])

def stuff():
    product = pd.read_sql("SELECT * FROM product", go_dwh.conn_db2)
    sales_staff = pd.read_sql("SELECT * FROM sales_staff", go_dwh.conn_db3)
    return_reason = pd.read_sql("SELECT * FROM return_reason", go_dwh.conn_db2)
    course = pd.read_sql("SELECT * FROM course", go_dwh.conn_db3)
    product_type = pd.read_sql("SELECT * FROM product_type", go_dwh.conn_db2)
    product_line = pd.read_sql("SELECT * FROM product_line", go_dwh.conn_db2)

    # retailer inlezen

    retailer_site = pd.read_sql("SELECT * FROM retailer_site", go_dwh.conn_db1)
    retailer_contact = pd.read_sql("SELECT * FROM retailer_contact", go_dwh.conn_db1)
    retailer_segment = pd.read_sql("SELECT * FROM retailer_segment", go_dwh.conn_db1)
    retailer_type = pd.read_sql("SELECT * FROM retailer_type", go_dwh.conn_db1)
    age_group = pd.read_sql("SELECT * FROM age_group", go_dwh.conn_db1)
    retailer = pd.read_sql("SELECT * FROM retailer", go_dwh.conn_db1)
    retailer_headquarters = pd.read_sql("SELECT * FROM retailer_headquarters", go_dwh.conn_db1)
    sales_demographic = pd.read_sql("SELECT * FROM sales_demographic", go_dwh.conn_db1)
    country = pd.read_sql("SELECT * FROM country", go_dwh.conn_db1)
    sales_territory = pd.read_sql("SELECT * FROM sales_territory", go_dwh.conn_db1)

    #retailer merging

    retailer_site_contact = pd.merge(retailer_site, retailer_contact, on='RETAILER_SITE_CODE')
    retailer_site_contact_retailer = pd.merge(retailer_site_contact, retailer, on='RETAILER_CODE')
    retailer_site_contact_type = pd.merge(retailer_site_contact_retailer, retailer_type, on='RETAILER_TYPE_CODE')
    segment_headquarters = pd.merge(retailer_segment, retailer_headquarters, on="SEGMENT_CODE")
    retailer_site_contact_type_sh = pd.merge(segment_headquarters, retailer_site_contact_type, on="RETAILER_CODEMR")
    age_sales_demo = pd.merge(age_group, sales_demographic, on="AGE_GROUP_CODE")
    retailer_site_contact_type_sh_age = pd.merge(retailer_site_contact_type_sh, age_sales_demo, on="RETAILER_CODEMR")
    retailer_site_contact_type_sh_age_country = pd.merge(retailer_site_contact_type_sh_age, country, left_on="COUNTRY_CODE_x", right_on="COUNTRY_CODE")
    retailer_df = pd.merge(retailer_site_contact_type_sh_age_country, sales_territory, on="SALES_TERRITORY_CODE")

    #product merging

    product_type_line = pd.merge(product_line, product_type, on="PRODUCT_LINE_CODE")
    product_df = pd.merge(product, product_type_line, on="PRODUCT_TYPE_CODE")

    return retailer_df


def main():
    # print("Overzicht van alle werkelijke orders:")
    # for row in order_details.iterrows():
    #     print(row)

    # print("Overzicht van alle teruggebrachte items:")
    # for row in go_dwh.returned_item.iterrows():
    #     print(row)

    # print("Wat zijn de productie-kosten van onze producten?")
    # product.sort_values(by="PRODUCTION_COST", inplace=True)
    # print(product[["PRODUCT_NAME", "PRODUCTION_COST"]])

    # plt.xlabel('Producten')
    # plt.ylabel('Prijs')
    # plt.title('Staafdiagram van productiekosten')

    # plt.bar(product["PRODUCT_NAME"], product["PRODUCTION_COST"])

    # plt.show()

    query = "INSERT INTO retailer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    
    retailer_site_contact_type_sh_age = stuff()

    export_conn = pyodbc.connect(go_dwh.conn_str)
    export_cursor = export_conn.cursor()

    for index, row in retailer_site_contact_type_sh_age.iterrows():
        values = (
            row['RETAILER_SITE_CODE'],
            row['RETAILER_CODEMR'],
            row['COMPANY_NAME'],
            row['POSTAL_ZONE_x'],
            row['REGION_x'],
            row['CITY_x'],
            row['RETAILER_TYPE_EN'],
            row['RETAILER_TYPE_CODE'],
            row['COUNTRY_CODE_x'],
            row['COUNTRY_EN'],
            row['SEGMENT_CODE'],
            row['SEGMENT_NAME'],
            row['GENDER'],
            row['AGE_GROUP_CODE'],
            row['UPPER_AGE'],
            row['LOWER_AGE'],
            row['SALES_TERRITORY_CODE'],
            row['TERRITORY_NAME_EN']
        )         
        export_cursor.execute(query, *values)

    print(export_cursor.execute("SELECT * FROM retailer").fetchall())

    product = pd.read_sql("SELECT * FROM product", go_dwh.conn_db2)

    def format_date(date_str):
        date_obj = datetime.datetime.strptime(date_str, '%d-%m-%Y')
        formatted_date = date_obj.strftime('%Y-%m-%d')
        return formatted_date

    print(product.columns)
    for _, row in product.iterrows():
        sales_price = float(row['PRODUCTION_COST']) * (1 + float(row['MARGIN']))
        formatted_date = format_date(row['INTRODUCTION_DATE'])
        query = "INSERT INTO product VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" 
        values = (
            row['PRODUCT_NAME'],
            row['DESCRIPTION'],
            sales_price,
            row['LANGUAGE'],
            row['PRODUCTION_COST'],
            row['MARGIN'],
            formatted_date,
            row['PRODUCT_TYPE_CODE'],
            "",
            ""
        )         
        export_cursor.execute(query, *values)

    export_conn.commit()
    export_conn.close()


if __name__ == "__main__":
    main()
