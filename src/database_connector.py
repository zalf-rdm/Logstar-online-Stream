
import logging
import os

import psycopg2

class PSQL_DB:

    conn = None

    def __init__(self,conf):
        self.psql_host = conf["postgres_host"]
        self.psql_database = conf["postgres_database"]
        self.psql_user = conf["postgres_user"]
        self.psql_password = conf["postgres_password"]

    def connect(self):
        logging.info("Connecting to database {}@{} ...".format(self.psql_database,self.psql_host))
        try:
            self.conn = onn = psycopg2.connect( host=self.psql_host,
                                                database=self.psql_database,
                                                user=self.psql_user,
                                                password=self.psql_password
                                            )
            return True
        except:
            logging.error("Could not connect to database, please check credentials and retry ...")
            return False

    def disconnect(self):
        logging.debug("Disconnecting from database ...")
        self.conn.close()
        self.cur.close()

    def __build_create_command__(self,table_name,header):
        command = "CREATE TABLE \"{}\" (".format(table_name)
        for k in header.keys():
            if k == "date":
                command += "\"col_date\" date,".format(header[k])
            elif k == "time": # time is always the last
                command += "\"col_time\" time, PRIMARY KEY (col_date, col_time));".format(header[k])
            else:
                command += "\"{}\" float,".format(header[k])
        return command

    def create_table(self,station,header):
        logging.info("Creating database {}".format(station))
        self.cur = self.conn.cursor()
        command = self.__build_create_command__(station,header)
        try:
            self.cur.execute(command)
            logging.info("creating table using: {} successful ...".format(command))

        except:
            logging.warning("creating table using: {} failed ...".format(command))
        self.conn.commit()
        self.cur.close()

    def __build_insert_command__(self,table_name,row,header):
        keys = ""
        values = ""
        first = True
        header_list = list(header.keys())
        for k,v in row.items():
            if not first:
                keys += ","
                values += ","
            else: 
                first = False
            
            if k == "date":
                keys += " \"{}\"".format("col_date")

            elif k == "time":
                keys += " \"{}\"".format("col_time")
            else:
                keys += " \"{}\"".format(header[k])
            values += " \'{}\'".format(v)

        command = "INSERT INTO {} ({}) VALUES ({});".format(table_name,keys,values)
        return command

    def insert_data(self,station,dict_all):
        sucessful = 0
        self.cur = self.conn.cursor()
        
        for row in dict_all["data"]:
            command = self.__build_insert_command__(station,row,dict_all["header"])
            try:
                self.cur.execute(command)
                logging.debug("Execution of: \"{}\" sucessful ...".format(command))
                sucessful += 1
            except:
                logging.debug("Execution of: \"{}\" failed ...".format(command))
                return
        logging.info("Inserted {} / {} columns of data".format(sucessful,len(dict_all["data"])))
        self.conn.commit()
        self.cur.close()
