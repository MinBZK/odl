D3G, de Document Data Definition Generator maakt gebruik van een aantal conventies om op basis van allerlei informatie de dokumentatie op te bouwen en de SQL te genereren die de database opbouwt. Uitgangspunt is de tabelnaam, die in dit *geval bronbestand_attribuut_meta* is. Deze tabelnaam is uitgangspunt voor een aantal files die binnen `d3g` gebruikt worden.

## .csv bestanden
- bronbestand_attribuut_meta.csv - een .csb files die hieronder wordt gedokumenteerd. Deze bevat per attribuut in de tabel een rij in .csv formaat met definties. Bijvoorbeeld is het gegeven een persoonsgegeven of niet, wat is het datatype, enz. Deze beschrijvende informatie wordt weggeschreven naar een tabel met de naam (tabelnaam)_description, in dit voorbeeld *bronbestand_attribuut_meta_description*. 
- bronbestand_attribuut_meta.meta.csv - bevat metainformatie over deze tabel; wordt weggeschreven naar de tabel *bronbestand_attribuut_meta_meta*.
- bronbestand_attribuut_meta.data.csv - bevat de data voor de vulling van deze tabel; komt terecht in de tabel *bronbestand_attribuut_meta_data*.

## .md bestanden
De .csv bestanden worden ook gebruikt om tabellen te genereren voor dokumentatie. Deze dokumentatie geeft analisten antwoord op vragen als uit welke tabel moet ik mijn gegevens ophalen, wat is het datatype en wat betekenen die kolommen? Deze tabellen zijnh hieronder te bewonderen. De dokumentatie wordt gegenereerd in het markdown formaat. Een eenvoudige manier om snel nette tekst te genereren. Per tabel kunnen twee .md files (markdown) files worden gegenereerd:
- bronbestand_attribuut_meta.prefix.md - de tekst die je nu leest. Als deze file wordt aangetroffen dan wordt deze als eerste, voor de tabel zelf in de dokumentatietekst gevoegd. Dat is een alternatief op het attribuut `Bronbestand_beschrijving` uit het metabestand `bronbestand_attribuut_meta.meta.csv`. Die tekst wordt altijd direkt boven de tabel gevoegd. De prefix optie geeft de mogelijkheid om een veel uitgebreidere inleiding te geven op de tabel.
bronbestand_attribuut_meta.suffix.md - tekst die aan het eind van de tabel wordt geplakt. Nuttig voor voorbeelden en dergelijke.

## Plaats van de bestanden
D3G kent twee directories: de `root_dir` waar al je relevante bestanden staan: src, data, maar ook schemas en docs. De `root_dir` is onderdeel van je gitlab project. Omdat de .csv bestanden van de schemas relevante informatie bevatten wordt sterk aangeraden deze mee te laten tracken met de rest van je informatie, ondanks dat het .csv bestanden zijn.

De benodigde bestanden worden uit `rootdir` ingelezen, bewerkt en vervolgens weggeschreven naar `workdir`. Beide directories specificeer je in de `config.yaml` file. De .csv bestanbden specificeer je in `schemas/odl` van de `rootdir`. Idem voor de dokumentatie.

Hieronder de `config.yaml` die gebruikt wordt voor het verwerken van de ODL schemas.

```
POSTGRES_HOST: 10.10.12.6
POSTGRES_DB: techniek
POSTGRES_SCHEMA: datamanagement
POSTGRES_PORT: 5432

ROOT_DIR: /data/arnoldreinders/projects/techniek/datamanagement
WORK_DIR: /data/arnoldreinders/projects/techniek/datamanagement/workdir

# mandatory sub directories in WORK_DIR
SUBDIRS:
  - data
  - docs
  - done
  - logs
  - schemas
  - sql
  - todo

# Data suppliers: translated into sub directories for each SUBDIRS in WORK_DIR
SUPPLIERS:
  - odl

DOC: logic-model.md
SQL: logic-model.sql

# tables and schema definition files, each table has a corresponding definition file
TABLES:
  bronbestand_attribuut_meta: {from: bronbestand_attribuut_meta.csv}
  bronbestand_bestand_meta: {from: bronbestand_bestand_meta.csv}
  bronbestand_datakwaliteit_feit: {from: bronbestand_datakwaliteit_feit.csv}
  bronbestand_datakwaliteit_codes: {from: bronbestand_datakwaliteit_codes.csv}
  bronbestand_levering_feit: {from: bronbestand_levering_feit.csv}

# columns to show in documentation, empty list means show all columns
COLUMNS: []
```
