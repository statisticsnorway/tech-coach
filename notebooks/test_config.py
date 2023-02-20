#######################################################
#        test_config.py              #
#######################################################

# Dette scriptet inneholder konfigurasjonene som bestemmer hvilke kildedata som
# leses i notebookene og hvor data skal lagres i strukturen på
# Google Cloud.

# Definisjon av evt. generasjon:
gen = "2021"

# Eks. hvis man oppererer på ulike 'tverrsnitt', snapshot i forløpsdata:
tverrsnitt = "20221121"

# 'versjon' er versjonen av datasettet som skal lagres på Google Cloud. Før
# publisering av statistikk vil versjonen alltid være 'v1' - men hvis det er
# behov for å endre datasettet i ettertid, eksempelvis med ny release på
# GitHub, må datasettet lagres som 'v2, 'v3' osv.:
versjon = "v1"

# De følgende pathene definerer den faste mappestrukturen på Google Cloud og
# bør ikke endres:

KILDE = "kilde1_data"
STATISTIKK = "test_statistikk1"

# Faste mapper:
KILDE_BUCKET = "gs://ssb-prod-tech-coach-data-kilde"  # kun siden vi ikke har noe automatisk innsamling som legger noe på INNDATA_PATH
PROD_BUCKET = "gs://ssb-prod-tech-coach-data-produkt"
DELT_PATH_BUCKET = "gs://ssb-prod-tech-coach-data-delt"


KILDE_PATH = f"{KILDE_BUCKET}/{KILDE}/g{gen}"
INNDATA_PATH = f"{PROD_BUCKET}/{KILDE}/inndata/g{gen}"
# eller hele filen?
# INNDATA_PATH = f"{PROD_BUCKET}/{KILDE}/inndata/g{gen}/filnavn_p{gen}_{versjon}.parquet"

KLARGJORT_PATH = f"{PROD_BUCKET}/{STATISTIKK}/klargjorte-data/g{gen}"
TVERRSNITT_PATH = f"{PROD_BUCKET}/{STATISTIKK}/temp/tverrsnitt/g{gen}"

AVLEDNING_PATH = f"{PROD_BUCKET}/temp/avledning/"

# Filsti til endelig statistikk
STATISTIKK_PATH = f"{PROD_BUCKET}/{STATISTIKK}/statistikk/g{gen}"

AVLEDNING_PATH = f"{PROD_BUCKET}/{STATISTIKK}/temp/avledning/g{gen}"
