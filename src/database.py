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

    def create(self, data: list[str], table_name: str = "table") -> None:
        header = data[0].split(',')
        rows = data[1:]

        table_query = """CREATE TABLE `{}`
            (`id` INTEGER NOT NULL PRIMARY KEY,
            `date_time` DATETIME NOT NULL,
            `customer_name` VARCHAR(255) NOT NULL,
            `customer_email` VARCHAR(255) NOT NULL,
            `product_name` VARCHAR(31) NOT NULL,
            `product_price` FLOAT NOT NULL)
        """.format(table_name)

        try:
            self._execute(table_query)
        except mysql.connector.errors.ProgrammingError:
            print(f"FEJL: Kunne ikke oprette tabel. Tabellen '{table_name}' eksisterer allerede.")
        else:
            print(f"SUCCES: Oprettede tabellen '{table_name}'.")

        # Det virker ikk', det her (%s, %(blabla)s, `%s`-formatet virker ikke, selvom det er det, der skal sikre mod SQL injection)
        # for row in rows:
        #     cols = row.split(',')
        #     insert_query = """INSERT INTO {} (`id`, `date_time`, `customer_name`, `customer_email`, `product_name`, `product_price`)"
        #         VALUES
        #            ROW(%(id)s, %(date_time)s, %(customer_name)s, %(customer_email)s, %(product_name)s, %(product_price)s)
        #     """.format(table_name)

        #     insert_params = {
        #         "table": table_name,
        #         "id": int(cols[0]),
        #         "date_time": datetime.datetime.fromisoformat(cols[1]),
        #         "customer_name": cols[2],
        #         "customer_email": cols[3],
        #         "product_name": cols[4],
        #         "product_price": float(cols[5])
        #     }
        #     self._execute(insert_query, insert_params)

    def read(self, table_name, column_name: str = None) -> dict[str]:
        """
        Læser data fra en tabel.
        """
        if column_name is None:
            select_query = "SELECT * FROM `%(table)s`"
            result = self._execute(select_query, { "table": table_name })
        else:
            select_query = "SELECT `%(column)s` FROM `%(table)s`"
            result = self._execute(select_query, { "table": table_name, "column": column_name })

        return result

    def update(self):
        pass

    def delete(self, table_name: str):
        """
        Fjerner en tabel helt.
        """
        drop_query = "DROP TABLE {}".format(table_name)

        if input(f"Er du sikker på, at du gerne vil fjerne tabellen '{table_name}' fuldstændigt? (j/N) ").lower() == 'j':
            self._execute(drop_query, { "name": table_name })
            print(f"Tabellen '{table_name}' blev fjernet.")
    
    def empty(self, table_name: str):
        """
        Rydder en tabel for al data, men fjerner ikke tabellen.
        """
        truncate_query = "TRUNCATE TABLE {}".format(table_name)

        if input(f"Er du sikker på, at du gerne vil rydde tabellen '{table_name}' for data? (j/N) ").lower() == 'j':
            self._execute(truncate_query)
            print(f"Tabellen '{table_name}' blev ryddet for data.")

    def reset(self) -> None:
        """
        Sletter database og gendanner derefter tom database med samme navn.
        """
        reset_query = "DROP DATABASE {}".format(self.database)
        if input(f"Er du sikker på, at du gerne vil nulstille databasen '{self.database}'? (j/N) ").lower() == 'j':
            self._execute(reset_query)
            self.create_database(self.connection, self.database)
            print(f"Databasen '{self.database}' blev nulstillet.")
    
    def _execute(self, query: str, params: dict[str] = None) -> None:
        with self.connection.cursor() as cursor:
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

if __name__ == "__main__":
    database = Database(database="testdb", username="root", password=open("secret.txt", "r", encoding="UTF-8").readline())
    raw_data = util.read_csv("orders_combined.csv")
    table_name = util.get_name("orders_combined.csv")
    database.create(raw_data, table_name)
