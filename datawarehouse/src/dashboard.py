import pandas as pd

import go_dwh


order_details = pd.read_sql_query("SELECT * FROM order_details", go_dwh.conns[1])
# go_dwh.returned_item


def main():
    print("Overzicht van alle werkelijke orders:")
    for row in order_details.iterrows():
        print(row)

    print("Overzicht van alle teruggebrachte items:")
    for row in go_dwh.returned_item.iterrows():
        print(row)


if __name__ == "__main__":
    main()
