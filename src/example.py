import database

def main() -> None:
    question = input("Vil du se previews af de SQL-queries, der køres? (j/N): ")
    show_queries = True if question.lower() in ['j', 'y'] else False

    # Forbinder til (eller opretter) databasen 'testdb',
    # indlæser automatisk de tre datasæt og omdanner til tabeller
    # 
    testdb = database.Database(
        username='',
        password='',
        database="testdb",
        init_load=["orders.csv", "customers.csv", "products.csv"],
        preview=show_queries
    )

    # Udvælger specifikke kolonner og ændrer rækkefølge
    order_example = testdb.read(
        "customers",
        "id", "email",
        order=1, direction='d',
        limit=19, offset=8
    )
    print("Resultat af order: ", order_example)

    # Joiner de tre tabeller
    first_join = {
        "right": "customers",
        "on_left": "customer",
        "on_right": "id",
        "join_type": 'i'
    }
    second_join = {
        "right": "products",
        "on_left": "product",
        "on_right": "id",
        "join_type": 'i'
    }
    join_example = testdb.read(
        "orders",
        "orders.id", "customers.name", "products.name",
        joins=[first_join, second_join]
    )
    print("Resultat af join: ", join_example)

    # Tilføjer keys til tabellerne
    for table in ["orders", "customers", "products"]:
        testdb.primary_key(table, "id")
    testdb.foreign_key("orders", { "customer": "customers.id", "product": "products.id" })

    # Lukker forbindelsen
    testdb.logout()
    # Genopretter forbindelsen
    testdb.login()
    # Nulstiller databasen
    testdb.reset()

    # Lukker igen
    testdb.logout()

if __name__ == "__main__":
    main()