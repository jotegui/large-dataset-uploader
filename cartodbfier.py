import psycopg2
import sys

from settings import *

# GBIF table
prefix = "gbif_sep2014"
latitudeField = "decimalLatitude"
longitudeField = "decimalLongitude"

# eBird
#prefix = "ebird_sep2014"
#latitudeField = "latitude"
#longitudeField = "longitude"

# PostgreSQL connection

conn = psycopg2.connect("host=mol.cartodb.com dbname=cartodb_user_b4ba2644-9de0-43d0-86fb-baf3b484ccd3_db user=cartodb_user_b4ba2644-9de0-43d0-86fb-baf3b484ccd3 options='-c statement_timeout=0'")
cur = conn.cursor()

# Calculate count of partition tables
cur.execute("select relname from pg_class where relkind='r' and relname like '{0}_%'".format(prefix))
cnt = len([x[0] for x in cur.fetchall()])

# Table names
partitions = list(range(cnt))
table_prefix = prefix
master_table = "{0}".format(table_prefix)
partition_tables = []
for i in partitions:
    partition = "{0}_{1}".format(prefix, i)
    partition_tables.append(partition)



#Remove inheritance
for partition in partition_tables:
    q = "alter table {0} no inherit {1}".format(partition, master_table)
    cur.execute(q)

# Check master table is empty
cur.execute("select count(*) from {0}".format(master_table))
test = cur.fetchone()[0]
if test != 0:
    print "Master table is NOT empty. Check for remaining inheritance"
    sys.exit()



#Cartodbfy master table
q = "select CDB_CartodbfyTable('{0}');".format(master_table)
cur.execute(q)



# Cartodbfy partition tables
for partition in partition_tables:
    q = "select CDB_CartodbfyTable('{0}');".format(partition)
    cur.execute(q)



# Update the geom fields
for partition in partition_tables:
    q = "update {0} set the_geom=ST_SetSRID(ST_Point({1}, {2}),4326), the_geom_webmercator=ST_Transform(ST_SetSRID(ST_Point({1}, {2}),4326), 3857) where {1} is not null and {2} is not null and {2}>-90 and {2}<90 and {1}>-180 and {1}<180;".format(partition, longitudeField, latitudeField)
    cur.execute(q)



# Re-inherit
for partition in partition_tables:
    q = "alter table {0} inherit {1}".format(partition, master_table)
    cur.execute(q)



# Committing changes
conn.commit()
cur.close()
conn.close()
