import pymysql
import csv
import logging
import json


class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """
        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        #JUST CREATE COL PREPRESETNATIONS, AND THEN THE TO_JSON ALLOWS FOR EASY INSERTION IN NEXT PART, ITTERATE
        self.column_name = column_name
        self.column_type = column_type
        self.not_null = not_null



    def __str__(self):
        return ""
        

    def to_json(self):
        """
        :return: A JSON object, not a string, representing the column and it's properties.
        """

        json_data = {"column_name": self.column_name, "column_type": self.column_type, "not_null": self.not_null}
        return json_data


class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type):
        """
        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        self.index_name = index_name
        self.kind = index_type

class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None):
        """
        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
            May be just a subset of the columns.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        """
        self.json_data = dict()
        self.t_name = t_name
        self.csv_f = csv_f
        self.column_definitions = column_definitions
        self.index_definitions = index_definitions
        self.cnx = cnx 

        #self.columns = []

        if t_name and csv_f:
            insert_table_definition = "INSERT INTO TableDefinitions(table_name, file_name) VALUES(%s, %s)"
            try:
                with self.cnx.cursor() as cursor:
                    cursor.execute(insert_table_definition, (t_name, csv_f))

                    self.cnx.commit()
            except Exception as e:
                print("Second created failed with e =  DataTableException: code: -101 , message: Table name %s is duplicate", t_name)

        

    def __str__(self):
        pass

    @classmethod
    def load_table_definition(cls, cnx, table_name):
        """
        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """
        description_query = """SELECT table_name as name, file_name as path FROM TableDefinitions WHERE table_name = %s"""
        columns_query = """SELECT column_name, column_type, not_null FROM ColumnDefinitions WHERE table_name = %s"""
        index_query = """SELECT index_name, columns, kind FROM IndexDefinitions WHERE table_name = %s"""

        columns = []
        indexes = dict()
        json_data = dict()

        try: 
            with cnx.cursor() as cursor:
                
                cursor.execute(description_query, (table_name))
                result = cursor.fetchone()
                csv_f = result["path"]
                json_data["defintion"] = result

                cursor.execute(columns_query, (table_name))
                result = cursor.fetchall()
                for item in result:
                    columns.append(item)
                json_data["columns"] = columns

                cursor.execute(index_query, (table_name))
                result = cursor.fetchall()
                for item in result:
                    index_data = {"index_name": item["index_name"], "columns": item["columns"].split(","), "kind": item["kind"]}
                    indexes[item["index_name"]] = index_data
                json_data["indexes"] = indexes

                table = cls(table_name, csv_f, columns, indexes, cnx)
                cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)

        return table

    def add_column_definition(self, c):
        """
        Add a column definition.
        :param c: New column. Cannot be duplicate or column not in the file.
        :return: None
        """
        #ADD COL DEF COLUM HERE, WITH SELF.TABLE_NAME AND AELF.FILENAME


        insert_column_definition = """INSERT INTO ColumnDefinitions(
                                            table_name, file_name, column_name, column_type, not_null) 
                                        VALUES(%s, %s, %s, %s, %s)"""
        with open(self.csv_f, "rU") as file:
            reader = csv.reader(file, delimiter = ",")
            column_names = next(reader)

            c = c.to_json()
            if c["column_name"] not in column_names:
                raise Exception("DataTableException: code: -100 , message: Column canary definition is invalid.")

        try:
            with self.cnx.cursor() as cursor:
                cursor.execute(insert_column_definition, (self.t_name, self.csv_f, c["column_name"], c["column_type"], c["not_null"]))
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)

    def drop_column_definition(self, c):
        """
        Remove from definition and catalog tables.
        :param c: Column name (string)
        :return:
        """
        drop_column_definition = """DELETE FROM ColumnDefinitions WHERE column_name = %s"""
        try:
            with self.cnx.cursor() as cursor:
                cursor.execute(drop_column_definition, (c))
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)

    def to_json(self):
        """
        :return: A JSON representation of the table and it's elements.
        """
        return self.json_data


    def define_primary_key(self, columns):
        """
        Define (or replace) primary key definition.
        :param columns: List of column values in order.
        :return:
        """
        insert_index_definition = """INSERT INTO IndexDefinitions(
                                            table_name, file_name, index_name, columns, kind) 
                                        VALUES(%s, %s, %s, %r, %s)"""
        columns_defined = []
        try:
            with self.cnx.cursor() as cursor:
                get_columns = "SELECT column_name FROM ColumnDefinitions where table_name = %s"
                cursor.execute(get_columns, (self.t_name))
                result = cursor.fetchall()
                for item in result:
                    columns_defined.append(item["column_name"])
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)

        for column in columns:
            if column not in columns_defined:
                raise Exception("DataTableException: code: Invalid key columns, message: -1000")


        try:
            with self.cnx.cursor() as cursor:
                #var_string = '_'.join(columns[i] for i in range(len(columns)))#.split(",")
                #print("HI", var_string)
                cursor.execute(insert_index_definition, (self.t_name, self.csv_f, "PRIMARY", ', '.join(columns[i] for i in range(len(columns))), "PRIMARY"))
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)


    def define_index(self, index_name, columns, kind="index"):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param kind: One of the valid index types.
        :return:
        """
        insert_index_definition = """INSERT INTO IndexDefinitions(
                                            table_name, file_name, index_name, columns, kind) 
                                        VALUES(%s, %s, %s, %r, %s)"""
        columns_defined = []
        try:
            with self.cnx.cursor() as cursor:
                get_columns = "SELECT column_name FROM ColumnDefinitions where table_name = %s"
                cursor.execute(get_columns, (self.t_name))
                result = cursor.fetchall()
                for item in result:
                    columns_defined.append(item["column_name"])
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)


        for column in columns:
            if column not in columns_defined:
                raise Exception("DataTableException: code: Invalid key columns, message: -1000")


        try:
            with self.cnx.cursor() as cursor:
                cursor.execute(insert_index_definition, (self.t_name, self.csv_f, index_name, ', '.join(columns[i] for i in range(len(columns))), kind))
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)


    def drop_index(self, index_name):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :return:
        """
        pass

    def get_index_selectivity(self, index_name):
        """
        :param index_name: Do not implement for now. Will cover in class.
        :return:
        """

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """
        description_query = """SELECT table_name as name, file_name as path FROM TableDefinitions WHERE table_name = %s AND file_name = %s"""
        columns_query = """SELECT column_name, column_type, not_null FROM ColumnDefinitions WHERE table_name = %s AND file_name = %s"""
        index_query = """SELECT index_name, columns, kind FROM IndexDefinitions WHERE table_name = %s AND file_name = %s"""

        columns = []
        indexes = dict()

        try: 
            with self.cnx.cursor() as cursor:
                cursor.execute(description_query, (self.t_name, self.csv_f))
                result = cursor.fetchone()
                self.json_data["defintion"] = result

                cursor.execute(columns_query, (self.t_name, self.csv_f))
                result = cursor.fetchall()
                for item in result:
                    columns.append(item)
                self.json_data["columns"] = columns

                cursor.execute(index_query, (self.t_name, self.csv_f))
                result = cursor.fetchall()
                for item in result:
                    index_data = {"index_name": item["index_name"], "columns": item["columns"][1:-1].split(","), "kind": item["kind"]}
                    indexes[item["index_name"]] = index_data
                self.json_data["indexes"] = indexes

                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)
        return self.to_json()

    

class CSVCatalog:

    def __init__(self, dbhost="localhost", dbport="3306", dbname="CSVCatalog", dbuser="dbuser", dbpw="dbuser", debug_mode=None):

        self.cnx = pymysql.connect(host=dbhost,
                             user=dbuser,
                             password=dbpw,
                             db=dbname,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

        drop_definitions = "DROP TABLE IF EXISTS TableDefinitions"
        drop_indexes = "DROP TABLE IF EXISTS IndexDefinitions"
        drop_columns = "DROP TABLE IF EXISTS ColumnDefinitions"

        create_table_definitions = """CREATE TABLE IF NOT EXISTS TableDefinitions (
                                        table_name varchar(255) PRIMARY KEY,
                                        file_name text NOT NULL
                                    );"""
        create_column_definitions = """CREATE TABLE IF NOT EXISTS ColumnDefinitions (
                                        table_name varchar(255) NOT NULL,
                                        file_name text NOT NULL,
                                        column_name varchar(255) NOT NULL,
                                        column_type enum("text", "number"),
                                        not_null tinyint(1)
                                    );"""
        create_index_definitions = """CREATE TABLE IF NOT EXISTS IndexDefinitions (
                                        table_name text NOT NULL,
                                        file_name text NOT NULL,
                                        index_name varchar(255) PRIMARY KEY,
                                        columns text NOT NULL,
                                        kind enum("PRIMARY", "UNIQUE", "INDEX") NOT NULL
                                    );"""

        try: 
            with self.cnx.cursor() as cursor:
                #cursor.execute(drop_definitions)
                #cursor.execute(drop_columns)
                #cursor.execute(drop_indexes)
                cursor.execute(create_table_definitions)
                cursor.execute(create_index_definitions)
                cursor.execute(create_column_definitions)
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)
        #pass

    def __str__(self):
        pass

    def create_table(self, table_name, file_name, column_definitions=None, primary_key_columns=None):

        table = TableDefinition(table_name, file_name, column_definitions, primary_key_columns, self.cnx)
        if column_definitions:
            for column_definition in column_definitions:
                table.add_column_definition(column_definition)
        if primary_key_columns:
            table.define_primary_key(primary_key_columns)

        return table

    def drop_table(self, table_name):
        drop_table_query1 = "DELETE FROM TableDefinitions WHERE table_name = %s"
        drop_table_query2 = "DELETE FROM ColumnDefinitions WHERE table_name = %s"
        drop_table_query3 = "DELETE FROM IndexDefinitions WHERE table_name = %s"

        try:
            with self.cnx.cursor() as cursor:
                cursor.execute(drop_table_query1, (table_name))
                cursor.execute(drop_table_query2, (table_name))
                cursor.execute(drop_table_query3, (table_name))
                self.cnx.commit()
        except pymysql.MySQLError as be:
            args = be.args
            print("Got exception = ", be)


    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return:
        """
        table = TableDefinition()
        table = table.load_table_definition(self.cnx, table_name)
        return table
        