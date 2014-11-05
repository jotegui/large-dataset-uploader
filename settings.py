__author__ = '@jotegui'

# Import modules in lib folder
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

# Example: eBird processing

# Data source: "ebird" or "gbif"
source = "ebird"

# Name of the main data table in RedShift
redshift_table = "ebird"

# Slim list of fields and their type
fields = [
    ["global_unique_identifier", "varchar(256)"],
    ["latitude", "double precision"],
    ["longitude", "double precision"],
    ["scientific_name", "varchar(256)"],
    ["observation_date", "date"],
    ["effort_distance_km", "double precision"],
    ["effort_area_ha", "double precision"]
]

# Name of the local folder to store the slim slices
slices_folder = "slim"

# Name of the field that contains the scientific name in Redshift
sciname_field = "scientific_name"

# Maximum number of rows in each partition table
threshold = 10000000
