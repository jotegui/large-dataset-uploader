__author__ = '@jotegui'

from settings import *
from cred import *

import sys
import json
import psycopg2
import requests
from subprocess import call

from Grouper import Grouper


class PointProcessor():
    """Upload a slim version of the table in RedShift to CartoDB."""
    
    
    def __init__(self):
        """Initialize class instance."""
        self.build_names()
        self.create_connections()
        return
    
    
    def main(self):
        """The whole process."""
        
        # First part, download slim version of data.
        self.part_1()
        
        # Second part, create the partitioned table infrastructure.
        self.part_2()
        
        print "Finished"
        
        return
    
    
    def part_1(self):
        """First part, download slim version of data."""
        
        # 1. Create bucket in Amazon S3 by calling the s3cmd command.
        self.create_s3_bucket()
        
        # 2. Populate the unload.sql template with the adequate values.
        self.create_unload_sql()
        
        # 3. Run the unload.sql query to download slim version of data from RedShift to S3.
        self.run_unload()
        
        # 4. Download the slices in S3 to local file system.
        self.download_slices()
        
        # 5. Delete the content of the Amazon S3 bucket, and the bucket itself.
        self.delete_s3_bucket()
        
        return
    
    
    def part_2(self):
        """Second part, create the partitioned table infrastructure."""
        
        # 1. Create master table in CartoDB using UI. 
        self.create_master_table()
        
        # 2. Add the columns to master table using the CartoDB SQL API.
        self.alter_master_table()
        
        # 3. Create the partitioned tables scaffolding
        self.scaffolding()
        
        # 4. Upload slices
        self.upload()
        
        return
    
    
    ############################
    # INITIALIZATION FUNCTIONS #
    ############################
    
    
    def build_names(self):
        """Build all variable names based on settings."""
        
        # Build empty dictionary to store names
        names = {}
        
        # Base name
        names['source'] = source
        
        # S3 bucket name and URI
        names['bucket_name'] = '{0}_slim'.format(source)
        names['bucket_uri'] = 's3://{0}'.format(names['bucket_name'])
        
        # RedShift table and field names
        names['redshift_table'] = redshift_table
        names['fields'] = [x[0] for x in fields]
        names['fields_complete'] = fields
        
        # Local folder for slim slices
        names['slices_folder'] = slices_folder
        
        # Store names dictionary in instance attribute
        self.names = names
        
        return
    
    
    def create_connections(self):
        """Create instances of psycopg2.connection to RedShift and CartoDB."""
        
        self.rs_conn=psycopg2.connect(host=rs_host,
                                      port=rs_port,
                                      dbname=rs_dbname,
                                      user=rs_user,
                                      password=rs_password)
        
        self.cdb_conn=psycopg2.connect(host=cdb_host,
                                       port=cdb_port,
                                       dbname=cdb_dbname,
                                       user=cdb_user,
                                       password=cdb_password,
                                       options='-c statement_timeout=0')
        
        self.rs_cur = self.rs_conn.cursor()
        self.cdb_cur = self.cdb_conn.cursor()
        
        return
    
    
    ####################
    # PART 1 FUNCTIONS #
    ####################
    
    
    def create_s3_bucket(self):
        """Create bucket in Amazon S3 by calling the s3cmd command."""
        
        r = call(['s3cmd', 'mb', self.names['bucket_uri']])
        
        return
    
    
    def create_unload_sql(self):
        """Populate the unload.sql template with the adequate values."""
        
        template = open('./unload.sql').read()
        fields_string = ', '.join(self.names['fields'])
        
        self.unload = template.format(fields_string, 
                                      self.names['redshift_table'],
                                      self.names['bucket_uri'],
                                      aws_access_key_id,
                                      aws_secret_access_key,
                                      sciname_field)
        
        return
    
    
    def run_unload(self):
        """Run the unload.sql query to download slim version of data from RedShift to S3."""
        
        self.rs_cur.execute(self.unload)
        
        return
    
    
    def download_slices(self):
        """Download the slices in S3 to local file system."""
        
        folder_string = './{0}'.format(self.names['slices_folder'])
        
        # Create folder
        call(['mkdir', folder_string])
        
        # Download slices
        call(['s3cmd', 'get', '--recursive', self.names['bucket_uri'], folder_string])
        
        # Check with user if files were downloaded
        print "============================"
        print "WARNING: User input required"
        print "============================"   
        print ""
        print "Here is the list of files that were downloaded. Please, check if the download finished properly."

        call(['ls', '-lah', folder_string])
        cont = False
        while cont is False:
            resp = raw_input("Did all the files make it to the local system? (Y/n) ")
            if resp.lower().startswith('y') or resp == "":
                cont = True
            elif resp.lower().startswith('n'):
                call(['rm', '-R', folder_string])
                self.download_slices()
                cont = True
            
        return
    
    
    def delete_s3_bucket(self):
        """Delete the content of the Amazon S3 bucket, and the bucket itself."""
        
        r = call(['s3cmd', 'rb', '-r', self.names['bucket_uri']])
        
        return
    
    
    ####################
    # PART 2 FUNCTIONS #
    ####################
    
    
    def create_master_table(self):
        """Create master table in CartoDB using UI."""
        
        agree = False
        print "============================"
        print "WARNING: User input required"
        print "============================"
        print
        print "To access the point table from the CartoDB UI, the table must be created from the web page."
        print "Please, go to https://mol.cartodb.com/dashboard/tables and create a new empty table. Just the empty table, the script will create the required columns."

        while agree is False:
            print "Name the table something like {0}_points and enter the name you used:".format(self.names['source'])
            self.names['master_table'] = raw_input("Used name (default {0}_points) > ".format(self.names['source']))
            if self.names['master_table'] == "":
                self.names['master_table'] = "{0}_points".format(self.names['source'])

            print "Process will continue, using {0} as the name of the master table.".format(self.names['master_table'])
            agree = raw_input("Agree? (Y/n) ")
            if agree == "" or agree.lower().startswith('y'):
                agree = True
            else:
                agree = False
        print "Using {0} as master table".format(self.names['master_table'])
        
        return
    
    
    def alter_master_table(self):
        """Add the columns to master table using the CartoDB SQL API."""
        
        url = "https://mol.cartodb.com/api/v2/sql"
        q = "alter table {0} add column ".format(self.names['master_table'])
        q += ", add column ".join([x[0]+" "+x[1] for x in fields])
        
        params = {'q':q, 'api_key':api_key}
        r = requests.get(url, params=params)
        
        if r.status_code != 200:
            if json.loads(r.content)['error'][0] == 'relation \"{0}\" does not exist'.format(self.names['master_table']):
                print "ERROR: Master table does not exist."
                print "Please, make sure you have typed the name correctly."
                self.create_master_table()
                self.alter_master_table()
            else:
                print "Something went wrong updating the master table."
                print json.loads(r.content)['error'][0]
                sys.exit()
        
        return
    
    
    def scaffolding(self):
        """Call Grouper.py to prepare the partition tables."""
        
        g = Grouper(self.names['master_table'])
        g.main()
        
        return
    
    
    def upload(self):
	"""Upload slices to CartoDB."""
        
        slices = os.listdir('./{0}'.format(self.names['slices_folder']))
        for slice in slices:
            print "Loading slice {0}".format(slice)
            with open('./{0}/{1}'.format(self.names['slices_folder'], slice)) as inp:
                self.cdb_cur.copy_from(inp, self.names['master_table'], null='', columns=self.names['fields'])
                self.cdb_conn.commit()
        return


if __name__ == "__main__":
    
    p = PointProcessor()
    p.main()
