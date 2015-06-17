__author__ = '@jotegui'

# Import modules in lib folder
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

# Example: eBird processing

# Data source: "ebird" or "gbif"
source = "gbif"

# Name of the main data table in RedShift
redshift_table = "gbif_jun_15"

# Slim list of fields and their type
fields = [
    ["gbifid", "bigint"],
    ["decimallatitude", "double precision"],
    ["decimallongitude", "double precision"],
    ["coordinateaccuracy", "varchar(256)"],
    ["species", "varchar(256)"],
    ["eventdate", "varchar(20)"],
    ["year", "integer"],
    ["datasetkey", "varchar(256)"],
    ["basisofrecord", "varchar(256)"]
]

# Name of the local folder to store the slim slices
slices_folder = "slim"

# Name of the field that contains the scientific name in Redshift
sciname_field = "species"

# Names of the coordinate fields
latitude_field = "decimallatitude"
longitude_field = "decimallongitude"

# Maximum number of rows in each partition table
threshold = 10000000
