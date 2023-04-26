from helpers.database_helpers import DatabaseHelper
from helpers.db import DatabaseConnection
from helpers.file_helper import WriteFile
from helpers.read_schema import ReadSchema
from helpers.write_csv import WriteCSV
from utils.log import logger


class DatabaseCheckHandler():
    def __init__(self):
        self.connection_handler = DatabaseConnection()
        self.read_schema_handler = ReadSchema()
        self.db_schema_handler = DatabaseHelper()
        self.conn = self.connection_handler.db_connection()
        self.writer = WriteFile()
        self.writer.check_folder_exist()
        self.cur = self.conn.cursor()
        self.data = dict()
        self.functions_procedure_data = dict()

    def tables_schema(self):
        try:
            logger.info("Open Connection With Database")
            logger.info("Getting All Tables from Schema file")
            schema = self.read_schema_handler.read_schema("schema/schema.sql")
            statements = schema.split(";")
            # statements will contains all Create Table statements with all columns for each Table
            statements = [stmt.strip() for stmt in statements if stmt.strip()]
            # get all tables from database
            logger.info("Getting All Tables from PostgreSQL")
            self.cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            tables = [table[0] for table in self.cur.fetchall()]

            logger.info(f"{len(statements)} Table will be check from Schema file")
            logger.info(f"{len(tables)} Table will be check from Database")
            # create source and destination list for the tables
            source_table_list = list()  # for schema file tables
            destination_table_list = list()  # for database tables
            matching_table_list = list()
            # start Checking if the tables in the database match the tables in the schema file
            for statement in statements:
                # get table name from the query
                if "CREATE TABLE" in statement:
                    source = statement.split('\n')
                    # build dictionary with all columns and it's type that exist in Schema file
                    source_schema = self.db_schema_handler.get_columns_type_schema(source[2:len(source)-1])
                    table_name = statement.split()[2]
                    if table_name in tables:
                        # add the table name that exist in both side schema file and Database
                        source_table_list.append(table_name)
                        destination_table_list.append(table_name)
                        matching_table_list.append(True)
                        logger.info(f"Getting Information for Table {table_name} Schema from PostgreSQL")
                        # get the table details from database
                        destination_schema = self.db_schema_handler.get_columns_type(self.cur, table_name)
                        logger.info("Start checking if the schema is valid")
                        # compare between both schema to check the match between columns
                        self.db_schema_handler.check_valid_schema(table_name, source_schema, destination_schema)
                    if table_name not in tables:
                        logger.info(f"Table {table_name} does not exist in database.")
            # get the table that exist in database and not exist in schema file
            non_existing_table = list(set(tables) ^ set(destination_table_list))
            destination_table_list.extend(non_existing_table)
            none_value = [None for i in range(len(destination_table_list) - len(source_table_list))]
            match_value = [False for i in range(len(destination_table_list) - len(source_table_list))]
            source_table_list.extend(none_value)
            matching_table_list.extend(match_value)
            # build the dictionary to build the csv file
            self.data['Source'] = source_table_list
            self.data['Destination'] = destination_table_list
            self.data['Matching'] = matching_table_list
            WriteCSV().build_csv("tables_result", self.data)
        except Exception as e:
            logger.info(f"Getting error in Main Process with Error {e}")
            self.cur.close()
            self.conn.close()
            exit()

    def function_procedure_schema(self):
        try:
            schema = self.read_schema_handler.read_schema("schema/func_proc_schema.sql")
            statements = schema.split(";")
            # statements will contains all Create Function/Procedure statements with all columns for each Table
            statements = [stmt.strip() for stmt in statements if stmt.strip()]
            logger.info("Getting All Function and Procedure from PostgreSQL")
            # get all Function and Procedure from database
            self.cur.execute("SELECT proname FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname='public')")
            functions_procedure_names = [record[0] for record in self.cur.fetchall()]
            logger.info(f"{len(functions_procedure_names)} Function/Procedure will be check")
            source_functions_procedure_list = list()  # for schema file functions/procedure
            destination_functions_procedure_list = list()  # for database functions/procedure
            destination_functions_procedure_type = list()
            destination_functions_procedure_matching = list()
            # start Checking if the functions/procedure in the database match the functions/procedure in the schema file
            for statement in statements:
                # get functions/procedure name from the query
                if "CREATE OR REPLACE" in statement:
                    # get the type (Function, Procedure) and tha name for it from query
                    func_proc_type = statement.split()[3]
                    func_proc_name = statement.split()[4].lower()
                    if func_proc_name in functions_procedure_names:
                        # add the Function/Procedure name that exist in both side schema file and Database
                        source_functions_procedure_list.append(func_proc_name)
                        destination_functions_procedure_list.append(func_proc_name)
                        destination_functions_procedure_type.append(func_proc_type.upper())
                        destination_functions_procedure_matching.append(True)
                        logger.info(f"{func_proc_type.upper()} {func_proc_name} exist in database.")
                    elif func_proc_name not in functions_procedure_names:
                        logger.info(f"{func_proc_type.upper()} {func_proc_name} does not exist in database.")
            # get the Function/Procedure that exist in database and not exist in schema file
            non_existing_functions_procedure = list(set(functions_procedure_names) ^ set(destination_functions_procedure_list))
            destination_functions_procedure_list.extend(non_existing_functions_procedure)
            source_none_value = [None for i in range(len(destination_functions_procedure_list) - len(source_functions_procedure_list))]
            type_none_value = ['Procedure/Function' for i in range(len(destination_functions_procedure_list) - len(source_functions_procedure_list))]
            matching_none_value = [False for i in range(len(destination_functions_procedure_list) - len(source_functions_procedure_list))]
            source_functions_procedure_list.extend(source_none_value)
            destination_functions_procedure_type.extend(type_none_value)
            destination_functions_procedure_matching.extend(matching_none_value)
            # build the dictionary to build the csv file
            self.functions_procedure_data['Type'] = destination_functions_procedure_type
            self.functions_procedure_data['Source'] = source_functions_procedure_list
            self.functions_procedure_data['Destination'] = destination_functions_procedure_list
            self.functions_procedure_data['Matching'] = destination_functions_procedure_matching
            WriteCSV().build_csv("function_procedure_result", self.functions_procedure_data)
            self.cur.close()
            self.conn.close()
        except Exception as e:
            logger.info(f"Getting error in Main Process with Error {e}")
            self.cur.close()
            self.conn.close()
            exit()
