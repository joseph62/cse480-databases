"""
Name: Sean Joseph
Time To Completion: 5 hours
Comments:
    I'm satisfied with the way I implemented create and insert
    functionality. The column object converting it's given data
    was a cool concept. I'm not sure if that is actually good design though.

    My implementation of the select statement is not as clean I feel it should be. 
    The order of operations for that needs to be reviewed because the way
    I'm tracking columns changing positions for sorting is not at all clean.

    It was cool being able to leverage python's built in sort functionality
    though. defining a function to select the columns involved in the sort felt good.

Sources:
    Python Docs
"""
import string
from enum import Enum 
from operator import itemgetter

_ALL_DATABASES = {}


class Connection:
    def __init__(self, filename):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self._filename = filename
        self._database = Database(filename)
        global _ALL_DATABASES
        _ALL_DATABASES[filename] = self._database
        

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        statement.replace(";","")
        tokens = self.parse_query(statement)
        results = None
        if tokens[0] == "CREATE" and tokens[1] == "TABLE":
            results = self.do_create(tokens)
        elif tokens[0] == "INSERT" and tokens[1] == "INTO":
            results = self.do_insert(tokens)
        elif tokens[0] == "SELECT":
            results = self.do_select(tokens)
        else:
            results = "Error"
        return results

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass

    def do_create(self,tokens):
        tokens = tokens[2:]
        table_name = tokens.pop(0)
        columns = self.parse_columns(tokens)
        table = Table(table_name,columns)
        self._database.add_table(table)
        return self._database.has_table(table.name)

    def do_insert(self,tokens):
        tokens = tokens[2:]
        table_name = tokens.pop(0)
        # Get rid of VALUES key word
        tokens.pop(0)
        record = [ arg[0] for arg in self.parse_args_list(tokens) ]
        return self._database.insert_record(table_name,record)

    def do_select(self,tokens):
        tokens = tokens[1:]
        columns = self.parse_selected_columns(tokens)
        table_name = tokens[tokens.index("FROM")+1]
        order_cols = [ x[0] for x in  self.parse_args_list(tokens[tokens.index("ORDER")+2:-1]) ]
        return self._database.get_records(table_name,cols=columns,order=order_cols)

    def parse_args_list(self,tokens):
        results = []
        column_args = []
        for token in tokens:
            if token == "(":
                continue
            elif token == ")":
                # Process column args and stop
                break
            elif token == ",":
                # Process column args
                results.append(column_args)
                # reset args
                column_args = []
            else:
                column_args.append(token)
        results.append(column_args)
        return results

    def parse_selected_columns(self,tokens):
        columns = []
        for token in tokens:
            if token == "FROM":
                break
            elif token == ",":
                continue
            elif token == "*":
                # Return None for all columns
                columns = None
                break
            else:
                columns.append(token)
        return columns

    def parse_columns(self,tokens):
        results = [ create_column(name,_type) for name,_type in self.parse_args_list(tokens) ]
        return results

    def parse_query(self,query):
        results = []
        token = ""
        in_string = False
        for char in query:
          if char in " \n\t " and not in_string:
            if len(token) > 0:
              results.append(token)
              token = ""
          elif char in "'":
            if in_string:
              results.append(token)
              token = ""
            in_string = not in_string
              
          elif char in "(,);" and not in_string:
            if len(token) > 0:
              results.append(token)
            results.append(char)
            token = ""
          else:
            token = token + char
        if len(token) > 0:
          results.append(token)
        return results

def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)


class Database:
    """
    Models a database
    filename : The filename for this database
    """
    def __init__(self, filename):
        self._filename = filename
        self._tables = {}

    @property
    def filename(self):
        return self._filename

    def has_table(self,table_name):
        """
        returns true if table_name is in the list of tables
        """
        return table_name in self._tables

    def add_table(self,table):
        self._tables[table.name] = table

    def insert_record(self,table_name,args):
        return self._tables[table_name].insert(args)

    def get_records(self,table_name,cols=None,pred=None,order=None):
        return self._tables[table_name].get(cols,pred,order)

class Table:
    """
    Models a table in a database
    name : Name of the table
    columns : A list of column objects
    """
    def __init__(self,name,columns):
        self._name = name
        self._columns = columns
        self._rows = []

    @property
    def name(self):
        return self._name

    @property
    def columns(self):
        return self._columns

    def insert(self,args):
        converted_args = []
        for column,arg in zip(self._columns,args):
            converted_args.append(column.convert(arg))
        self._rows.append(tuple(converted_args))
        return True

    def get_selected_columns_index(self,selected_cols):
            index_columns = []
            for col in selected_cols:
                for index,table_column in enumerate(self._columns):
                    if col == table_column.name:
                        index_columns.append(index)
            return index_columns

    def get(self,cols=None,pred=None,order=None):
        # Rethink order of operations/structure of this.
        # Right now we have to do some weird state tracking to
        # order columns properly if column order changes.
        selected = []
        updated_col_order = None
        if cols is None:
            selected = self._rows
        else:
            updated_col_order = self.get_selected_columns_index(cols)
            for row in self._rows:
                result = []
                for col in updated_col_order:
                    result.append(row[col])
                selected.append(tuple(result))

        if order is not None:
            order_indexes = []
            for order_index in order:
                for index,col in enumerate(self._columns):
                    if order_index == col.name:
                        if updated_col_order is None:
                            order_indexes.append(index)
                        else:
                            order_indexes.append(updated_col_order.index(index))
                        break
            def deref_row(row):
                sort_on = []
                for index in order_indexes:
                    sort_on.append(row[index])
                return sort_on
            selected.sort(key=deref_row)
                    
        return selected

    def __str__(self):
        return "{}: ({})".format(self._name,self._columns)
    __repr__ = __str__

class Column:
    """
    Models a column in a table
    name : Name of the column
    type : The type of data that goes in this column
    """
    def __init__(self,name,_type):
        self._name = name
        self._type = _type

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    def convert(self,arg):
        converted = None
        if arg == "NULL":
            pass
        elif self._type == ColumnType.INTEGER:
            converted = int(arg)
        elif self._type == ColumnType.REAL:
            converted = float(arg)
        elif self._type == ColumnType.TEXT:
            converted = arg
        return converted

    def __str__(self):
        return "{}({})".format(self._name,self._type)
    __repr__ = __str__

class ColumnType(Enum):
    NULL = 1
    INTEGER = 2
    REAL = 3
    TEXT = 4

def create_column(name, str_type):
    type_map = {
            "INTEGER" : ColumnType.INTEGER,
            "REAL" : ColumnType.REAL,
            "TEXT" : ColumnType.TEXT,
        }

    return Column(name, type_map[str_type])

if __name__ == '__main__':
    conn = connect("test.db")
    conn.execute("CREATE TABLE students (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
    conn.execute("INSERT INTO students VALUES (33, 'hi', 4.5);")
    conn.execute("INSERT INTO students VALUES (3, 'hweri', 4.5);")
    conn.execute("INSERT INTO students VALUES (75842, 'string with spaces', 3.0);")
    conn.execute("INSERT INTO students VALUES (623, 'string with spaces', 3.0);")
    result = conn.execute("SELECT * FROM students ORDER BY col_3_, col_1;")
    print(result)
    
