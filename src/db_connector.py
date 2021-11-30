
import pyodbc
import logging
import psycopg2

class DBConnector:

    conn = None

    def __init__(self,conf):
        self.conf = conf

    def __do_connect__():
        pass

    def connect(self):
        """ Opens an active connection to the database"""

        logging.info("Try to connect to database {}://{}@{}:{}/{}".format(self.conf["db_driver"],
                                                                          self.conf["db_username"],
                                                                          self.conf["db_host"],
                                                                          self.conf["db_port"],
                                                                          self.conf["db_database"]))
        try:
            self.__do_connect__()
            return True
        except:
           logging.error("Could not connect to database, please check credentials and retry ...")
           return False

    def disconnect(self):
        logging.debug("Disconnecting from database ...")
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __build_create_command__(self,table_name,header):
        column_names = ""
        first = True
        for k in header.keys():
            if k == "date" or k == "time":
                continue
            
            if not first:
                column_names += ","
            else:
                first = False
            column_names += "\"{}\" float".format(header[k])
        
        command = "CREATE TABLE \"{}\" ({}, \"timestamp\" timestamp PRIMARY KEY);".format(table_name,column_names)
        return command

    def __build_insert_command__(self,table_name,row,header):
        keys = ""
        values = ""
        first = True
        header_list = list(header.keys())
        for k,v in row.items():
            if k == "date" or  k == "time":
                continue

            if not first:
                keys += ","
                values += ","
            else: 
                first = False
            
            keys += " \"{}\"".format(header[k])
            if v is None:
                values += " \'{}\'".format(self.NULLVALUE)
            else:
                values += " \'{}\'".format(v)
        timestamp = row['date'] + " " + row['time']
        keys += ", \"timestamp\""
        values += ", \'{}\'".format(timestamp)
        command = "INSERT INTO \"{}\" ({}) VALUES ({});".format(table_name,keys,values)
        return command

    def create_table(self,station,header):
        self.cur = self.conn.cursor()
        logging.info("Creating table {}".format(station))
        command = self.__build_create_command__(table_name=station,header=header)
        try:
            self.cur.execute(command)
            self.conn.commit()
            logging.info("creating table using: {} successful ...".format(command))
        except pyodbc.DatabaseError as err:
            logging.warning("creating table using: {} failed ...".format(command))
        except psycopg2.errors.DuplicateTable:
            logging.warning("creating table using: {} already exists ...".format(command))

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
            finally:    
                self.conn.commit()
        logging.info("Inserted {} / {} columns of data".format(sucessful,len(dict_all["data"])))


class MSSQLConnector(DBConnector):
    NULLVALUE = "null"

    def __do_connect__(self):
        self.conn = pyodbc.connect( DRIVER=self.conf["db_driver"],
                                    SERVER=self.conf["db_host"],
                                    PORT=self.conf["db_port"],
                                    UID=self.conf["db_username"],
                                    PWD=self.conf["db_password"],
                                    SCHEMA=self.conf["db_database"],
                                    autocommit=True)

class PSQLConnector(DBConnector):
    NULLVALUE = "NaN"

    def __do_connect__(self):
        self.conn = psycopg2.connect( host=self.conf["db_host"],
                                            database=self.conf["db_database"],
                                            user=self.conf["db_username"],
                                            password=self.conf["db_password"],
                                            port=self.conf["db_port"]
                                        )
