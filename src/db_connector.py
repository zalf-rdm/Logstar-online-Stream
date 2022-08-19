
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
            logging.info("connected ...")
            return True
        except:
           logging.error("Could not connect to database, please check credentials and retry ...")
           return False
        
    def disconnect(self):
        logging.debug("Disconnecting from database ...")
        if self.conn is not None:
            self.conn.close()
            self.conn = None

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
