import util
import connector
import getpass
import datetime
import decimal

class Database(connector.DatabaseConnector):
    """
    >>> Database("root", "hunter2", "testdb")

    :param username:
        *Påkrævet*. Standardværdi: ``''``
    :type username: str
    :param password:
        *Påkrævet*. Standardværdi: ``''``
    :type password: str
    :param database: 
        *Upåkrævet*. Standardværdi: ``''``
    :type database: str
    :param preview: Bestemmer, om queries skal forhåndsvises inden eksekvering.
        *Upåkrævet*. Standardværdi: ``True``
    """
    def __init__(self,
        username: str = '',
        password: str = '',
        database: str = '',
        preview: bool = True
    ) -> None:
        """
        Konstruktøren af database-objektet.
        """
        # Konfiguration
        self.preview = preview
        # Initialiserer connectoren
        super().__init__(username, password, database)
        # Hvis forbindelsen ikke kan skabes (f.eks. fordi det angivne databasenavn ikke eksisterer),
        # kan brugeren forsøge at oprette en database med navnet.
        if self.database and not self.connection:
            new = input(f"Vil du forsøge at oprette en ny database med navnet '{self.database}'? (j/N): ")
            if new.lower() == 'j':
                self.create_database(self.database)
                # Skal logge ind igen for at forny forbindelserne
                self._first_login(getpass.getpass("Indtast adgangskode igen: "))

    def _execute(self,
        query: str,
        params: dict[str] | list[dict[str]] = {},
        db: bool = True,
        read: bool = False,
    ) -> bool | list[tuple]:
        """
        Eksekverer et SQL-query.

        :param query: Queriet, der skal eksekveres. Skal skrives i SQL.
            *Påkrævet*.
        :type query: str
        :param params: En dict eller liste af dicts indeholdende parameteriserede værdier
            bestemt af brugeren, der skal indsættes sikkert i queriet,
            bl.a. for at undgå SQL injection.
            *Upåkrævet*. Standardværdi: ``{}``
        :type params: dict[str] | list[dict[str]]
        :param db: Bestemmer om handlingen udføres i en specifik database eller direkte.
            Skal være ``False`` ved f.eks. oprettelse af ny database eller nulstilning af database.
            *Påkrævet*. Standardværdi: ``True``
        :type db: bool
        :param read: Bestemmer, om der skal bruges en buffered cursor,
            så data kan læses og fetches fra databasen.
            *Upåkrævet*. Standardværdi: ``False``
        :type read: bool

        :return: Queriet kunne eksekveres, og handlingen blev gennemført problemfrit.
        :rtype: bool: ``True``
        :return: Handlingen kunne ikke gennemføres.
        :rtype: bool: ``False``
        :return: Den læste data fra databasen, hvis en READ-operation kunne gennemføres.
        :rtype: list[tuple]
        """
        connection = self.connection if db else self.direct_connection
        try:
            with connection.cursor(buffered=True if read else False) as cursor:
                # Hvis 'params' er en liste, køres queriet for hver gruppe 'params'
                if isinstance(params, list):
                    cursor.executemany(query, params)
                # Ellers køres queriet kun én gang
                else:
                    cursor.execute(query, params)
            # Committer evt. ændringer i tabeller eller data
            connection.commit()
            # .__exit__() er implementeret for cursoren i mysql.connector,
            # så denne behøves ikke lukkes manuelt, når with-blokke bruges

            # Hvis i læsetilstand, returneres den læste data
            if read:
                return cursor.fetchall()
        except Exception as err:
            print(f"FEJL: Kunne ikke udføre handlingen. Følgende fejl opstod:\n    ", err)
            return False
        else:
            return True

    def _preview(self, query: str) -> None:
        if self.preview:
            msg = " > " + query
            title = "Forhåndsvisning af forespørgsel:"
            print('-' * max(len(msg), len(title)))
            print(title)
            input(msg)
            print('-' * max(len(msg), len(title)))

    # CREATE-operationer
    def create_database(self, database_name: str) -> None:
        """
        Opretter en database med det angivne navn.

        :param database_name: Navnet på databasen, der ønskes oprettet.
            *Påkrævet*.
        :type database_name: str
        """
        database_query = f"CREATE DATABASE `{database_name}`"

        self._preview(database_query)

        if self._execute(database_query, db=False):
            print(f"SUCCES: Databasen '{database_name}' blev oprettet.")

    def create(self, columns: str, table_name: str = "table") -> None:
        header = columns.strip('\n').split(',')

        # TODO: Omskriv denne del
        create_query = f"CREATE TABLE `{table_name}` ("
        # Gætter datatype ud fra kolonnenavn
        # Primary key vil ud fra de tre datasæt altid være et heltalligt id
        # Men det kan også være, at den skal gættes, hvis man vil kunne bruge f.eks. et navn som nøgle
        # Og et datasæt behøver jo ikke engang at have en primary key
        # Men det vile måske være smartere at gætte ud fra felternes værdi fra første række
        # Det kommer an på, om man følger en fast navngivningspraksis for kolonnerne i datasættene
        for column in header:
            create_query += f"`{column}` "
            if column == "id":
                create_query += "INTEGER NOT NULL PRIMARY KEY, "
            elif "name" in column:
                create_query += "VARCHAR(80) NOT NULL, "
            elif "email" in column:
                create_query += "VARCHAR(254) NOT NULL, "
            elif "price" in column:
                # For dette datasæt er (P=8,D=5) i DECIMAL(P,D)
                # Men for pengebeløb burde D vel egentlig være 2
                create_query += "DECIMAL(10,5) NOT NULL, "
            elif "date" in column:
                create_query += "DATETIME NOT NULL, "
            elif column in ["customer", "product"]:
                create_query += "INTEGER NOT NULL, "
        create_query = create_query[:-2] + ')'

        self._preview(create_query)

        if self._execute(create_query):
            print(f"SUCCES: Oprettede tabellen '{table_name}'.")

    # TODO: Forsøg at matche kolonnenavne fra dataens header med kolonnenavne fra den valgte tabel
    # TODO: Implementér et system til at skippe eller overwrite, hvis et felt i en række i datasættet
    # har samme værdi som ditto i tabellen. Hvis altså kolonnen har PRIMARY KEY eller UNIQUE som constraint.
    def insert(self,
        data: list[str],
        table_name: str,
        header: bool = True
    ) -> None:
        """
        Indsætter en eller flere rækker data i en tabel.

        :param data: Dataene, der ønskes indsat i tabellen.
            *Påkrævet*.
        :type data: list[str]
        :param table_name: Navnet på tabellen, som dataen skal indsættes i.
            *Påkrævet*.
        :type table_name: str
        :param header: Angiver, om datasættet, der inputtes, indeholder en header med kolonnenavne,
            som skal springes over.
            Hvis ``True`` forsøges dataen desuden at matches med den angivne tabels kolonnenavne.
            *Upåkrævet*. Standardværdi: ``True``
        :type header: bool

        :return: Hvis tabellen ikke findes, eller hvis dataene ikke har samme antal kolonner som tabellen.
        :rtype: None
        """
        # Benyttes ikke endnu, men kan bruges til at bytte rundt på kolonner,
        # hvis de står i en anden rækkefølge end tabellen, som dataene skal indsættes i
        if header:
            columns = data[0].strip('\n').split(',')
        # Springer over header
        rows = data[1:] if header else data
        # Opdeler hver række i felter
        rows = [row.strip('\n').split(',') for row in rows]

        # Henter info om tabellen
        table_info = self.info(table_name)
        if not table_info:
            return
        # Tjekker om alle rækker i den inputtede dataliste har samme antal felter, som der er kolonner i den angivne tabel,
        # dvs. om længden af hver række (list[str]) er den samme som længden af headerinfoen (list[tuple])
        if not all(len(row) == len(table_info) for row in iter(rows)):
            print("FEJL: En eller flere rækker data er uforenelig med tabellens format.")
            return

        # Danner query
        insert_query = f"INSERT INTO `{table_name}` ("
        # Kolonnenavne (med backticks, fordi navnene er taget fra tabellen)
        insert_query += ", ".join([f"`{column[0]}`" for column in table_info]) + ") VALUES ("
        # Kolonneværdier (med %()s, fordi det er værdier oplyst af brugeren, der skal tjekkes)
        insert_query += ", ".join([f"%({column[0]})s" for column in table_info]) + ')'

        # Danner dict over parametre til indsættelse af data
        insert_params = []
        for row in rows:
            insert_param = {column[0]: row[index] for index, column in enumerate(table_info)}
            # Konverterer str til passende datatype
            # for index, column in enumerate(table_info):
                # Koverteringen lader til at være spild af tid,
                # da værdiene også omdannes til de rette typer,
                # hvis man bare indsætter tekst...
                # insert_param[column[0]] = row[index]
                # Kun de to første værdier (navn og type) tages fra kolonneinfoen
                # column_name, column_type, *_ = column
                # if "int" in column_type:
                #     value = int(row[index])
                # elif "datetime" in column_type:
                #     value = datetime.datetime.fromisoformat(row[index])
                # # Dækker char, varchar, text og [adj.]text
                # elif "char" in column_type or "text" in column_type:
                #     value = row[index]
                # elif "float" in column_type:
                #     value = float(row[index])
                # elif "decimal" in column_type:
                #     value = decimal.Decimal(row[index])
                # insert_param[column_name] = value
            insert_params.append(insert_param)

        self._preview(insert_query)

        if self._execute(insert_query, insert_params):
            print(f"SUCCES: Data indsat i tabellen '{table_name}'.")

    # TODO: Ikke afprøvet endnu
    def new_table(self, data: list[str], table_name: str = "table", header: str = '') -> None:
        """
        :param data: Dataene, der danner grundlag for den nye tabel.
            *Påkrævet*.
        :type data: list[str]
        :param table_name: Navnet på tabellen, der ønskes oprettet.
            *Påkrævet*. Standardværdi: ``"table"``
        :type table_name: str
        :param header: En kommasepareret tekststreng indeholdende kolonnenavne.
            *Upåkrævet*. Standardværdi: ``''``
        :type header: str
        """
        if not header:
            header, *body = data
        self.create(header, table_name)
        self.insert(body, table_name, header=False)

    # READ-operationer
    def read(self,
        table_name: str,
        *column_name: str,
        order: int | str = 0,
        direction: str = 'a',
        limit: int = 0,
        offset: int = 0
    ) -> list[tuple] | None:
        """
        Læser data fra en tabel.

        :param table_name: Navnet på den tabel, som data skal læses fra.
            *Påkrævet*
        :type table_name: str
        :param column_name: Navnet eller navnene på den kolonne eller de kolonner, som data skal læses fra.
            *Upåkrævet*.
        :type column_name: str
        :param order: Kolonnen, som resultatet ordnes efter.
            Enten *int*, der vælger indekset af kolonnen blandt de valgte kolonner,
            eller *str*, der vælger ud fra navnet på kolonnen.
            *Upåkrævet*. Standardværdi: ``0``
        :type order: int | str
        :param direction: Angiver hvilken retning, resultatet skal ordnes i.
            ``'a'``, ``"asc"`` eller ``"ascending"`` er opadgående rækkefølge, mens
            ``'d'``, ``"desc"`` eller ``"descending"`` er nedadgående rækkefølge.
            *Upåkrævet*. Standardværdi: ``'a'``
        :type direction: str
        :param limit: Mængden af rækker, der skal læses.
            Når værdien er ``0``, læses alle resultater.
            *Upåkrævet*. Standardværdi: ``0``
        :type limit: int
        :param offset: Mængden af rækker, der springes over, inden læsningen påbegyndes.
            Når værdien er ``0``, læses alle resultater.
            *Upåkrævet*. Standardværdi: ``0``
        :type offset: int

        :return: En liste med rækker indeholdende data fra de(n) valgte kolonne(r).
        :rtype: list[tuple]
        :return: Hvis READ-operationen ikke kunne gennemføres.
        :rtype: None
        """
        select_params = {}

        select_query = "SELECT "
        if column_name:
            select_query += ", ".join([f"`{column}`" for column in column_name])
        else:
            select_query += '*'
        select_query += f" FROM `{table_name}`"

        # Tilføjer sorteringsretning
        select_query += self._sort(select_query, column_name, order, direction)

        # Tilføjer limit og offset
        limit_query, limit_params = self._limit(select_query, limit, offset)
        select_query += limit_query
        select_params.update(limit_params)

        self._preview(select_query)

        result = self._execute(select_query, select_params, read=True)
        if result:
            print(f"SUCCES: Dataene blev læst fra '{table_name}' problemfrit.")
            return result

    def _sort(self, query: str, column_name: str | tuple[str], order: int | str = 0, direction: str = 'a') -> str:
        query = ""
        if isinstance(order, int) and order >= 0 and order < len(column_name):
            query += f" ORDER BY `{column_name[order]}`"
        elif isinstance(order, str) and order in column_name:
            query += f" ORDER BY `{order}`"
        if "ORDER BY" in query:
            if direction.lower() in ['a', "asc", "ascending"]:
                query += " ASC"
            elif direction.lower() in ['d', "desc", "descending"]:
                query += " DESC"

        return query

    def _limit(self, query: str, limit: int, offset: int) -> tuple[str, dict[str]]:
        query = ""
        params = {}
        if isinstance(limit, int) and limit > 0:
            query += " LIMIT %(limit)s"
            params["limit"] = limit
        if isinstance(offset, int) and offset > 0:
            query += " OFFSET %(offset)s"
            params["offset"] = offset

        return query, params

    def info(self, table_name: str) -> list[tuple] | None:
        """
        Henter info om en tabels opbygning.

        :param table_name: Navnet på tabellen, hvis info efterspørges.
            *Påkrævet*.
        :type table_name: str

        :return: En liste indeholdende info om hver kolonne i tabellen, herunder navn og datatype.
        :rtype: list[tuple]
        :return: Hvis READ_operationen ikke kunne gennemføres,
        :rtype: None
        """
        describe_query = f"DESCRIBE `{table_name}`"

        self._preview(describe_query)

        table_info = self._execute(describe_query, read=True)
        if table_info:
            print(f"SUCCES: Hentede info om tabellen '{table_name}'.")
            return table_info

    # Opretter enten en samlet tabel eller returnerer
    def _join(self,
        left: str,
        right: str,
        on_left: str,
        on_right: str,
        type: str = 'i',
    ) -> list[tuple] | None:
        """
        :param left: Den venstre tabel.
            *Påkrævet*.
        :type left: str
        :param right: Den højre tabel.
            *Påkrævet*.
        :type right: str
        :param on_left: Navnet på kolonnen, som der joines på i venstre tabel
            *Påkrævet*.
        :type on_left: str
        :param on_left: Navnet på kolonnen, som der joines på i højre tabel
            *Påkrævet*.
        :type on_left: str
        :param direction: Typen af join. Kan være
            ``'i'``, ``'o'``, ``'l'``, ``'r'``,
            ``"inner"``, ``"outer"``, ``"left"`` eller ``"right"``.
            *Påkrævet*. Standardværdi: ``'i'``
        :type direction: str
        """
        pass

    # UPDATE-operationer
    def update(self) -> None:
        pass

    # DELETE-operationer
    # 
    def delete(self, table_name: str, force: bool = False) -> None:
        """
        Fjerner en tabel helt fra databasen.

        :param table_name: Navnet på tabellen, der ønskes fjernet.
            *Påkrævet*.
        :type table_name: str
        :param force: Bestemmer om bekræftelse af operation skal springes over.
            *Upåkrævet*. Standardværdi: ``False``
        :type force: bool
        """
        drop_query = f"DROP TABLE `{table_name}`"

        self._preview(drop_query)

        # Det er altid godt at bekræfte ved delete-operationer
        if not force:
            confirmation = input(f"Er du sikker på, at du gerne vil fjerne tabellen '{table_name}' fuldstændigt? (j/N) ")
        if force or confirmation.lower() == 'j':
            # Hvis query gennemføres problemfrit, printes positivt resultat
            if self._execute(drop_query):
                print(f"SUCCES: Tabellen '{table_name}' blev fjernet.")
    
    def empty(self, table_name: str, force: bool = False) -> None:
        """
        Rydder en tabel for al data, men fjerner ikke tabellen.

        :param table_name: Navnet på tabellen, der ønskes ryddet for data.
            *Påkrævet*.
        :type table_name: str
        :param force: Bestemmer om bekræftelse af operation skal springes over.
            *Upåkrævet*. Standardværdi: ``False``
        :type force: bool
        """
        truncate_query = f"TRUNCATE TABLE `{table_name}`"

        self._preview(truncate_query)

        # Det er altid godt at bekræfte ved delete-operationer
        if not force:
            confirmation = input(f"Er du sikker på, at du gerne vil rydde tabellen '{table_name}' for data? (j/N) ")
        if force or confirmation.lower() == 'j':
            # Hvis query genneføres problemfrit, printes positivt resultat
            if self._execute(truncate_query):
                print(f"SUCCES: Tabellen '{table_name}' blev ryddet for data.")

    def reset(self, force: bool = False) -> None:
        """
        Nulstiller databasen.

        Sletter nuværende database og gendanner derefter en tom database med samme navn.

        :param force: Bestemmer om bekræftelse af operation skal springes over.
            *Upåkrævet*. Standardværdi: False
        :type force: bool
        """
        drop_query = f"DROP DATABASE `{self.database}`"

        self._preview(drop_query)

        # Det er altid godt at bekræfte ved delete-operationer
        if not force:
            confirmation = input(f"Er du sikker på, at du gerne vil nulstille databasen '{self.database}'? (j/N) ")
        if force or confirmation.lower() == 'j':
            # Hvis begge queries gennemføres problemfrit, printes positivt resultat
            if self._execute(drop_query) and self.create_database(self.database):
                print(f"Databasen '{self.database}' blev nulstillet.")
                # Skal logge ind igen for at forny forbindelserne
                self.login()

if __name__ == "__main__":
    database = Database(database="testdb", username="root", password=open("secret.txt", "r", encoding="utf-8").readline())
    tables = ["orders.csv", "customers.csv", "products.csv"]
    for table in tables:
        raw_data = util.read_csv(table)
        table_name = util.get_name(table)
        database.create(raw_data[0], table_name)
        database.insert(raw_data, table_name)
    database.logout()
    database.login()
    print(database.info("customers"))
    print(database.read("customers", "id", "email", order=1, direction='d', limit=10, offset=23))
    database.reset()
    database.logout()


# Måske kunne man lave en Table class og så loade hver tabel ind i konstruktøren som
# self.orders, self.customers, self.products, osv.
# Table kunne så nedarve CRUD-operationerne
# (i modificeret udgave uden parameteren table_name selvfølgelig og med nogle func som attribut)
# Så ville man kunne sige testdb.orders.info eller testdb.customers.empty()
# eller testdb.products.create("color")
# Så ville det være smart at lave et objekt for hver kolonne,
# så man kunne sige testdb.orders.product.distinct(),
# og man ville kunne sige testdb.customers.customer_email[11]
# for at få emailadressen på kunden for ordre-id 11, hvis noget er gået galt med ordren
# Eller testdb.products[3] for at få hele rækken om produkt-id 3,
# eller testdb.products[3][2] alias testdb.products[3]["price"] for at få prisen
# Og nu begynder det at ligne pandas
