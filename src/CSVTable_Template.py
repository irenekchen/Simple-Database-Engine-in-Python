import csv  # Python package for reading and writing CSV files.

# You MAY have to modify to match your project's structure.
import DataTableExceptions
import CSVCatalog


import json

max_rows_to_print = 10


class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name

        # Holds loaded metadata from the catalog. You have to implement  the called methods below.
        self.__description__ = None
        self.__rows__ = None
        self.indexed_tables = {} # {field string: table}
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = None # list of dicts
            self.__load__()  # Load rows from the CSV file.

            # Build indexes defined in the metadata. We do not implement insert(), update() or delete().
            # So we can build indexes on load.
            self.__build_indexes__()
        else:
            self.__file_name__ = "DERIVED"


    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """
        table = CSVTable.__catalog__.get_table(self.__table_name__)
        self.__description__ = table.describe_table()
    

    # Load from a file and creates the table and data.
    def __load__(self):

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                # Get the names of the columns defined for this table from the metadata.
                column_names = self.__get_column_names__()

                # Loop through each line (well dictionary) in the input file.
                for r in reader:
                    # Only add the defined columns into the in-memory table. The CSV file may contain columns
                    # that are not relevant to the definition.
                    projected_r = self.project([r], column_names)[0]
                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    def __get_file_name__(self):
        description = self.__description__
        definition = description["defintion"]
        file_name = definition["path"]
        return file_name

    def __get_column_names__(self):
        column_names = []
        description  = self.__description__
        column_list = description["columns"]
        for column in column_list:
            column_name = column["column_name"]
            column_names.append(column_name)
        return column_names

    def __str__(self):
        """
        You can do something simple here. The details of the string returned depend on what properties you
        define on the class. So, I cannot provide a simple implementation.
        :return:
        """
        return self.__get_file_name__()

    def __build_indexes__(self):
        description = self.__description__
        indexes = description["indexes"] #is a dict of {"index_name": {INDEX_INFO}}
        for index_name, index_info in indexes.items():
            list_index_columns = index_info["columns"]
            index_key_string = '_'.join(list_index_columns[i] for i in range(len(list_index_columns)))
            #CREATE INDEX KEY STRING 
            #if (DO CHECK ON IF NEED CREATE INDEX) #and BEST INDEX (__GET_ACCESS_PATH__)
            # CREATE A DICT WITH {index_columns, row_id}
            if index_key_string not in self.indexed_tables.keys():
                self.indexed_tables[index_key_string] = dict()
            for rowID, row in enumerate(self.__rows__):
                projected_r = []
                for field in range(0, len(list_index_columns)):  # Make a new row with just the requested columns/fields.
                    v = row[list_index_columns[field]]
                    projected_r.append(v)
                #projected_r = self.project([r], list_index_columns)[0]
                row_key_string = '_'.join(projected_r[i] for i in range(len(projected_r)))
                if row_key_string not in self.indexed_tables[index_key_string].keys():
                    self.indexed_tables[index_key_string][row_key_string] = []
                self.indexed_tables[index_key_string][row_key_string].append(rowID)
                #row_dict = self.indexed_tables[index_key_string]
                #row_list.append(projected_r)
            """
            for (ROW IN SELF.__ROWS__):
                IF INDEX KEY STRING EXISTS IN DICT:
                    RETRIEVE VALUE LIST
                    APPEND TO VALUE LIST 
                    SET VALUE LIST BACK TO DICT
                ELSE 
                    CREATE NEW EMPTY VLUE LIST AND APPEND NEW VALUE
                    SET VALUE LIST BACK TO DICT

            """
            # CREATE A DICT WITH {index_columns, row_id}
        #pass

    def __get_access_path__(self, tmp):
        """
        Returns best index matching the set of keys in the template.
        Best is defined as the most selective index, i.e. the one with the most distinct index entries.
        An index name is of the form "colname1_colname2_coluname3" The index matches if the
        template references the columns in the index name. The template may have additional columns, but must contain
        all of the columns in the index definition.
        :param tmp: Query template.
        :return: Index or None
        """
        pass

    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """

        # Basically, this means there is no where clause.
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows  # Should really return a new, identical table but am lazy.
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}  # Not sure why I am using range.
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.

                return result #list that represents new row in dict form {column_header: row_val}

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")

    def __add_row__(self, projected_row):
        if not self.__rows__:
            self.__rows__ = []
        self.__rows__.append(projected_row)


    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise DataTableExceptions.DataTableException(-101, "Limit/offset not supported for CSVTable")

        # If there are rows and the template is not None
        if self.__rows__ is not None:

            result = []

            # Add the rows that match the template to the newly created table.
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):
        """
        Find using a selected index
        :param t: Template representing a where clause/
        :param idx: Name of index to use.
        :param fields: Fields to return.
        :param limit: Not implemented. Ignore.
        :param offset: Not implemented. Ignore
        :return: Matching tuples.
        """
        results = []
        index_fields_list = idx.split("_")
        template_list = []
        for field in index_fields_list:
            template_list.append(t[field])
        row_key = '_'.join(template_list[i] for i in range(len(template_list)))
        list_valid_rows = self.indexed_tables[idx][row_key]
        for row_num in list_valid_rows:
            row = self.__rows__[row_num]
            if self.matches_template(row, t):
                results.append(row)
        results = self.project(results, fields)
        return results

    def find_by_template(self, t, fields=None, limit=None, offset=None):
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.
        #looking by namelast
        valid_index = None
        valid_index_fields = 0
        if t:
            template_fields = t.keys()
            for index in self.indexed_tables:
                index_fields_set = set(index.split("_"))
                if template_fields == index_fields_set:
                    if len(index_fields_set) > valid_index_fields:
                        valid_index = index
                        valid_index_fields = len(index_fields_set)
            if valid_index:
                result = self.__find_by_template_index__(t, valid_index, fields, limit, offset)
            else:
                result = self.__find_by_template_scan__(t, fields, limit, offset)
            return result
        else:
            return self.__rows__

    def insert(self, r):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Insert not implemented"
        )

    def delete(self, t):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Delete not implemented"
        )

    def update(self, t, change_values):
        raise DataTableExceptions.DataTableException(
            code=DataTableExceptions.DataTableException.not_implemented,
            message="Updated not implemented"
        )

    def join(self, right_r, on_fields, where_template=None, project_fields=None, optimize=False):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """

        # If not optimizations are possible, do a simple nested loop join and then apply where_clause and
        # project clause to result.
        #
        # At least two vastly different optimizations are be possible. You should figure out two different optimizations
        # and implement them.
        #
        left_r = self #left table

        selected_l = self.find_by_template(where_template) #selected left rows
        # make into table....
        # selected_l = self.table_from_rows("") 
        selected_l = self.table_from_rows("LEFTSELECTED", selected_l)
        left_rows = selected_l.get_row_list()
        right_rows = right_r.get_row_list()
        result_rows = []

        left_rows_processed = 0
        for lr in left_rows:
            #projected_r = self.project([r], column_names)[0]
            #on_template = self.project(lr, on_fields)
            on_template = self.get_on_template(lr, on_fields)
            for rr in right_rows:
                if self.matches_template(rr, on_template):
                    new_r = {**lr, **rr}
                    result_rows.append(new_r)
            left_rows_processed += 1
            #if left_rows_processed % 10 == 0:
            #    print("Processed ", left_rows_processed, " left rows ... ")
            #if left_rows_processed == 200:
            #    join_result = self.table_from_rows("JOIN:" + left_r.__table_name__ + ":" + right_r.__table_name__, result_rows)
                #on_template = self.get_on_template
            #    return join_result.get_row_list()

        join_result = self.table_from_rows("JOIN:" + left_r.__table_name__ + ":" + right_r.__table_name__, result_rows)
            #on_template = self.get_on_template
        return join_result.get_row_list()

    def table_from_rows(self, table_name, rows):
        table = CSVTable(table_name, False)
        for row in rows:
            table.__add_row__(row)
        return table

    def get_row_list(self):
        return self.__rows__

    def get_on_template(self, row, on_template):
        #print(on_template)
        projected_r = dict()
        for field in on_template:  # Make a new row with just the requested columns/fields.
            v = row[field]
            projected_r[field] = v
        return projected_r





