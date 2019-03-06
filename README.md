# Simple-Database-Engine-in-Python
Implementation of a simple database engine on top of CSV files in Python

# Components
* CSV Catalog
* CSV Datatable Engine

# CSVCatalog
CSVCatalog implements a simple database catalog. The catalog defines three types:
* A TableDefinition represents metadata information about a CSVDataTable. The database engine will maintain the data in a CSV file. The catalog contains information about:
  * The path/file name for the data.
  * Column names, types and whether or not NULL is an allowed value. The column names defined via the catalog API are a subset of column headers in the underlying CSV file.
  * Columns that comprise the primary key.
  * A set of one or more index definitions. An index definition has a name, type of index (PRIMARY, UNIQUE, INDEX) and columns that comprise the index value.
* ColumnDefinition: A class defining a column.
* IndexDefinition: A class defining an index.

The catalog supports:
* Defining a new table.
* Dropping an existing table definition.
* Loading a previous defined table definition.
* Adding and removing columns from a table definition.
* Adding and removing indexes from a table definition.

The catalog information is stored in a set of tables predefined tables created in the MySQL workspace.

There is a function CSVCatalog.get_table(). This returns a previous created catalog description. 

# CSVDataTableEngine
Core components: 
* find_by_template() determines if there is an applicable index (access path) that can be used to accelerate a find_by_template(). If there is an applicable index, find_by_template uses the index by calling __find_by_template_index__(). The find is implemented using indexes as well as the logic to determine if there is an applicable index.
* execute_join() performs a join of the table (self) with the input table. The method includes a list of the on_columns. The column names are the same in both tables. There is also a where_template and field_list to apply to the result of the execute_join(). 
