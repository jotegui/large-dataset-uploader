__author__ = '@jotegui'

import psycopg2
from cred import *
from settings import *
import sys


class Grouper():
    """Defines groups for partitioned tables."""
    def __init__(self, master_table):
        
        # From settings module
        self.sciname_field = sciname_field
        self.table = redshift_table
        
        # From argument in call
        self.master_table = master_table
        
        self.groups = []
        self.limits = None
        self.l = 1
        self.trigger_conditions = ''
        
        # Credentials taken from cred.py
        self.rs_conn=psycopg2.connect(host=rs_host, port=rs_port, dbname=rs_dbname, user=rs_user, password=rs_password)
        self.cdb_conn=psycopg2.connect(host=cdb_host, port=cdb_port, dbname=cdb_dbname, user=cdb_user, password=cdb_password)
        
        self.rs_cur = self.rs_conn.cursor()
        self.cdb_cur = self.cdb_conn.cursor()
    
    
    def main(self):
        """Main process."""
        
        # First loop
        self.loop()        
        
        # Rest of loops
        while len(self.limits) > 0:
            self.loop()
        
        self.partition_tables()
        
        return
        
        
    def loop(self):
        """."""
        print "-------------------------------------"
        print "round {0}".format(self.l)
        self.get_counts()
        print "group"
        self.group()
        print "{0} groups so far...".format(len(self.groups))
        print "new limits = {0}".format(self.limits)
        print "new groups = {0}".format(self.groups)
        print "done"
        
        return
    
    
    def get_counts(self):
        """."""
        
        if self.limits is not None:
            limit_string = "where {0} like '".format(self.sciname_field)
            limit_string += "%' or {0} like '".format(self.sciname_field).join(self.limits)
            limit_string += "%'"
        else:
            limit_string = "where {0} is not null and {0} != ''".format(self.sciname_field)
        
        q = "select id, count(*) as alpha from (select {2}, substring({2} from 1 for {0}) as id from {3} {1}) as foo group by id order by count(*);".format(self.l, limit_string, self.sciname_field, self.table)
        self.rs_cur.execute(q)
        return
    
    
    def group(self):
        """."""
        ids = []
        cont = 0
        self.limits = []
        
        # First extraction
        t = self.rs_cur.fetchone()
        
        # Loop begins
        while t:
            
            cont += t[1]
            
            # Check if group is larger than threshold
            if cont >= threshold:
                
                # If just the first value is larger than threshold
                if len(ids) == 0:
                    self.l += 1
                    # Store remaining values in limit for next query
                    while t:
                        self.limits.append(t[0])
                        t = self.rs_cur.fetchone()
                    # Exit
                    return
                # Otherwise, store list of prefixes as a group
                else:
                    cont -= t[1]
                    self.groups.append(ids)
                    ids = []
                    cont = 0
            else:
                ids.append(t[0])
                t = self.rs_cur.fetchone()
        
        self.groups.append(ids)
        
        return
    
    
    def partition_tables(self):
        """."""
        prefix = 0
        for group in self.groups:
            
            # Create partition table
            with open('./create_template.sql') as o:
                create_template = o.read()
            check = "{0} like '".format(self.sciname_field)
            check += "%' or {0} like '".format(self.sciname_field).join(group)
            check += "%'"
            q = create_template.format(prefix, check, self.master_table)
            self.cdb_cur.execute(q)
            
            # Create indexes
            with open('./index_template.sql') as o:
                index_template = o.read()
            q = index_template.format(prefix, self.master_table, self.sciname_field)
            self.cdb_cur.execute(q)
            with open('./index_lower_template.sql') as o:
                index_lower_template = o.read()
            q = index_lower_template.format(prefix, self.master_table, self.sciname_field)
            self.cdb_cur.execute(q)

            # Create trigger function content
            if self.trigger_conditions == "":
                self.trigger_conditions += "IF ( NEW.{0} like '".format(self.sciname_field)
            else:
                self.trigger_conditions += " ELSIF ( NEW.{0} like '".format(self.sciname_field)
            self.trigger_conditions += "%' OR NEW.{0} like '".format(self.sciname_field).join(group)
            self.trigger_conditions += "%' ) THEN INSERT INTO {1}_{0} VALUES (NEW.*);".format(prefix, self.master_table)
            prefix += 1
        
        # Create trigger function
        with open('./trigger_function_template.sql') as o:
            trigger_function_template = o.read()
        q = trigger_function_template.format(self.trigger_conditions, self.master_table)
        self.cdb_cur.execute(q)
        
        # Create trigger
        q = "CREATE TRIGGER insert_{0}_trigger BEFORE INSERT ON {0} FOR EACH ROW EXECUTE PROCEDURE {0}_insert_trigger();".format(self.master_table)
        self.cdb_cur.execute(q)
        
        # Commit changes to db
        self.cdb_conn.commit()
        
        return

if __name__ == "__main__":
    threshold = 10000000  # groups of 10M rows max
    
    print "start"
    g = Grouper()
    
    g.main()
