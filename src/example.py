import database
import util

def main() -> None:
    # Hvis der vælges ja her, stoppes kørslen af scriptet midlertidigt, hver gang et query køres
    question = input("Vil du se previews af de SQL-queries, der køres? (j/N): ")
    show_queries = True if question.lower() in ['j', 'y'] else False

    ### EKSEMPEL 1 ###
    # Forbinder til (eller opretter) databasen 'testdb', indlæser automatisk de tre datasæt og omdanner dem til tabeller i databasen
    # Brugernavn og adgangskode er efterladt blanke i dette eksempel, så du kan forbinde til din egen MySQL-instans
    # NB! Databasenavnet er her sat til at være 'spac_testdb'. Sørg for, at dette navn ikke er brugt i forvejen, så eksemplerne her køres ordentligt
    # Alternativt kan du udskifte 'spac_testdb' med '' for at blive promptet om et navn, eller også kan du erstatte det med et andet navn, som du ønsker at bruge
    testdb = database.Database(
        username='',
        password='',
        database="spac_testdb",
        init_load=["orders.csv", "customers.csv", "products.csv"],
        preview=show_queries
    )
    # Tilføjer keys til tabellerne
    for table in ["orders", "customers", "products"]:
        testdb.primary_key(table, "id")
    testdb.foreign_key(
        table_name="orders",
        foreign_key={ "customer": "customers.id", "product": "products.id" }
    )
    # Viser info om databasen
    print(testdb.info())

    ### EKSEMPEL 2 ###
    # Udvælger specifikke kolonner at læse og ændrer i resultatets rækkefølge
    order_example = testdb.read(
        "customers",
        "id", "email",
        order=1, direction='d',
        limit=19, offset=8
    )
    print("Resultat af order:\n", order_example)

    ### EKSEMPEL 3 ###
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
    # Vi finder kundens navn og produktnavnet for hver ordre
    join_example = testdb.read(
        "orders",
        "orders.id", "customers.name", "products.name",
        joins=[first_join, second_join]
    )
    print("Resultat af join:\n", join_example)

    ### EKSEMPEL 4 ###
    # Opretter ny tabel manuelt
    testdb.create(
        columns="id,date_time,customer_name,customer_email,product_name,product_price",
        table_name="orders_combined",
        primary_key="id"
    )
    # Viser info om tabellen
    print(testdb.info("orders_combined"))
    # Indsætter data fra en fil
    testdb.insert(
        data=util.read_csv("orders_combined.csv"),
        table_name="orders_combined",
        header=True
    )
    # Kopierer queriet fra eksempel 3
    combined_example = testdb.read(
        "orders_combined",
        "id", "customer_name", "product_name",
    )
    print("Resultat af kopien af join:\n", combined_example)
    # Sammenligner det joinede eksempel og det kombinerede datasæt
    print("De to order-queries er ens:", join_example == combined_example)

    ### EKSEMPEL 5 ###
    # Lukker forbindelsen
    testdb.logout()
    # Genopretter forbindelsen uden at skulle oplyse info igen
    testdb.login()
    # Nulstiller databasen
    testdb.reset()

    # Lukker igen
    testdb.logout()

if __name__ == "__main__":
    main()