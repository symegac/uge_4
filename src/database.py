import util
import connector
import datetime
import mysql.connector

class Database(connector.DatabaseConnector):
    def __init__(self,
                 database: str = None,
                 username: str = None,
                 password: str = None
                ) -> None:
        super().__init__(database, username, password)

    def create(self, columns: str, table_name: str = "table") -> None:
        primary_key, *header = columns.split(',')

        # Primary key vil ud fra de tre datasæt altid være et heltalligt id
        # Men det kan også være, at den skal gættes, hvis man vil kunne bruge f.eks. et navn som nøgle
        create_query = """CREATE TABLE `{}`
            (`{}` INTEGER NOT NULL PRIMARY KEY,
        """.format(table_name, primary_key)

        # Gætter datatype ud fra kolonnenavn
        # Men det vile måske være smartere at gætte ud fra felternes værdi fra første række
        # Det kommer an på, om man følger en fast navngivningspraksis for kolonnerne i datasættene
        for column in header:
            create_query += f"`{column.strip()}` "
            if "name" in column or "email" in column:
                create_query += "VARCHAR(255) NOT NULL, "
            elif "price" in column:
                create_query += "FLOAT NOT NULL, "
            elif "date" in column:
                create_query += "DATETIME NOT NULL, "
        create_query = create_query[:-2] + ')'

        try:
            self._execute(create_query)
        except mysql.connector.errors.Error as err:
            print("FEJL: Følgende fejl opstod:\n", err)
        else:
            print(f"SUCCES: Oprettede tabellen '{table_name}'.")

    def insert(self, data: list[str], table_name: str = "table", header: bool = True) -> None:
        if header:
            primary_key, *columns = data[0].split(',')
        rows = data[1:] if header else data

        # TODO: Ændr ud fra primary_key og columns, så formatet kan tilpasse sig og ikke bare svarer til 'orders_combined'-dokumentet
        insert_query = """INSERT INTO `{}`
                (id, date_time, customer_name, customer_email, product_name, product_price)
            VALUES
                (%(id)s, %(date_time)s, %(customer_name)s, %(customer_email)s, %(product_name)s, %(product_price)s)
        """.format(table_name)
        insert_params = []

        for row in rows:
            cols = row.split(',')
            insert_param = {
                "id": int(cols[0]),
                "date_time": datetime.datetime.fromisoformat(cols[1]),
                "customer_name": cols[2],
                "customer_email": cols[3],
                "product_name": cols[4],
                "product_price": float(cols[5])
            }
            insert_params.append(insert_param)

        try:
            self._execute(insert_query, insert_params, many=True)
        except mysql.connector.errors.Error as err:
            print("FEJL: Følgende fejl opstod:\n", err)
        else:
            print(f"SUCCES: Data indsat i tabellen '{table_name}'.")

    # Ikke afprøvet endnu
    def new_table(self, data: list[str], table_name: str = "table", header: str = '') -> None:
        if not header:
            header, *body = data
        self.create(header, table_name)
        self.insert(body, table_name, header=False)

    # Kunne evt. lave *column_name for at vælge flere cols
    def read(self, table_name, column_name: str = '') -> list[str]:
        """
        Læser data fra en tabel.
        """
        select_query = "SELECT `{}` FROM `{}`".format(
            (column_name if column_name else '*'),
            table_name
        )
        result = self._execute(select_query, read=True)

        return result

    def update(self):
        pass

    def delete(self, table_name: str):
        """
        Fjerner en tabel helt.
        """
        drop_query = "DROP TABLE IF EXISTS `{}`".format(table_name)

        if input(f"Er du sikker på, at du gerne vil fjerne tabellen '{table_name}' fuldstændigt? (j/N) ").lower() == 'j':
            self._execute(drop_query, { "name": table_name })
            print(f"Tabellen '{table_name}' blev fjernet.")
    
    def empty(self, table_name: str):
        """
        Rydder en tabel for al data, men fjerner ikke tabellen.
        """
        truncate_query = "TRUNCATE TABLE `{}`".format(table_name)

        if input(f"Er du sikker på, at du gerne vil rydde tabellen '{table_name}' for data? (j/N) ").lower() == 'j':
            self._execute(truncate_query)
            print(f"Tabellen '{table_name}' blev ryddet for data.")

    def reset(self) -> None:
        """
        Sletter database og gendanner derefter tom database med samme navn.
        """
        reset_query = "DROP DATABASE `{}`".format(self.database)
        if input(f"Er du sikker på, at du gerne vil nulstille databasen '{self.database}'? (j/N) ").lower() == 'j':
            self._execute(reset_query)
            self.create_database(self.database)
            print(f"Databasen '{self.database}' blev nulstillet.")
    
    def _execute(self, query: str, params: dict[str] | list[dict[str]] = {}, many: bool = False, read: bool = False) -> None:
        """
        Eksekverer et SQL-query
        """
        with self.login() as connection:
            with connection.cursor(buffered=True if read else False) as cursor:
                if many:
                    cursor.executemany(query, params)
                else:
                    cursor.execute(query, params)
            connection.commit()

        if read:
            return cursor.fetchall()

if __name__ == "__main__":
    database = Database(database="testdb", username="root", password=open("secret.txt", "r", encoding="utf-8").readline())
    raw_data = util.read_csv("orders.csv")
    table_name = util.get_name("orders_combined.csv")
    # database.create(raw_data, table_name)
    print(database.read(table_name, "customer_name"))
    database.create(raw_data[0], "supertestb")
    database.insert(raw_data, "supertestb")
    # "customer_name FROM orders_combined; DROP TABLE aaa; --"
    # raw_data = util.read_csv("orders.csv")
    # database.create(raw_data, "orders.csv")
