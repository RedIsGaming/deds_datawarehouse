import pandas as pd

import go_dwh


order_details = pd.read_sql_query("SELECT * FROM order_details", go_dwh.conns[1])
product = pd.read_sql_query("SELECT * FROM product", go_dwh.conns[1])


def main():
    import matplotlib.pyplot as plt
    print("Overzicht van alle werkelijke orders:")
    for row in order_details.iterrows():
        print(row)

    print("Overzicht van alle teruggebrachte items:")
    for row in go_dwh.returned_item.iterrows():
        print(row)

    print("Wat zijn de productie-kosten van onze producten?")
    product.sort_values(by="PRODUCTION_COST", inplace=True)
    print(product[["PRODUCT_NAME", "PRODUCTION_COST"]])

    plt.xlabel('Producten')
    plt.ylabel('Prijs')
    plt.title('Staafdiagram van Productprijzen')

    plt.bar(product["PRODUCT_NAME"], product["PRODUCTION_COST"])

    plt.show()


if __name__ == "__main__":
    main()
