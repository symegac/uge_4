# SPAC Uge 4
## Setup (CLI)
1. Start med at hente en kopi af projektet (sørg for at være det sted i filsystemet, hvor du vil gemme projektet)
> git clone https://github.com/symegac/uge_4.git
2. Naviger til den nye mappe og lav et virtuelt miljø:
> cd uge_4
>
> py -m venv .venv
3. Aktivér miljøet i din foretrukne shell, her bash som eksempel (kør .bat-filen i samme mappe hvis cmd eller .ps1-filen hvis powershell):
> . .venv/Scripts/activate
4. Installér så dependencies:
> pip install -r requirements.txt
5. Nu er du klar til at køre Python-filerne! Jeg anbefaler at køre [example.py](src/example.py) for at se koden i aktion:
> py src/example.py

## Organisering
Der er fire filer i [src](src)-mappen:
1. [connector.py](src/connector.py) bruger mysql.connector til at oprette en forbindelse til en database.
2. [util.py](src/util.py) indeholder værktøjer til at læse en .csv-fil og få dens filnavn.
3. [database.py](src/database.py) indeholder Database-klassen, som opretter et objekt, hvorigennem man kan interagere med dataene i en database.
4. [example.py](src/example.py) er en fil, der udfører eksempler på interaktion med databasen. Her gennemgås nogle af de forskellige funktioner fra Database.