"""
Name: Sean Joseph
Time To Completion: 
Comments:
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
        self.filename = filename
        self.database = Database(filename)
        global _ALL_DATABASES
        _ALL_DATABASES[filename] = self.database
        

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        tokens = parse_query(statement)
        results = None
        if tokens[0] == "CREATE" and tokens[1] == "TABLE":
            results = self.do_create(tokens)
        elif tokens[0] == "INSERT" and tokens[1] == "INTO":
            tokens = tokens[2:]
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
        columns = parse_columns(tokens,table_name)
        table = Table(table_name,columns)
        self.database.add_table(table)
        return self.database.has_table(table.name)

    def do_insert(self,tokens):
        table_name = tokens.pop(0)
        # Get rid of VALUES key word
        columns = None
        if tokens[0] == "(":
            columns = [ column[0] for column in parse_args_list(tokens) ]
        tokens = tokens[tokens.index("VALUES")+1:]
        for record in parse_records(tokens):
            values = [ arg[0] for arg in parse_args_list(record) ] 
            self.database.insert_record(table_name,values,columns)


    def do_select(self,tokens):
        tokens = tokens[1:]
        columns = parse_selected_columns(tokens)
        table_name = tokens[tokens.index("FROM")+1]
        predicate = None
        if "WHERE" in tokens:
            pred_tokens = tokens[tokens.index("WHERE")+1:tokens.index("ORDER")]
            print(pred_tokens)
        order_cols = [ x[0] for x in parse_args_list(tokens[tokens.index("ORDER")+2:-1]) ]
        return self.database.get_records(table_name,cols=columns,order=order_cols)

def connect(filename):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename)

def parse_records(tokens):
    records = []
    while ")" in tokens and "(" in tokens:
        start = tokens.index("(")
        end = tokens.index(")")
        records.append(tokens[start:end+1])
        tokens = tokens[end+1:]
    return records

def parse_args_list(tokens):
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

def parse_selected_columns(tokens):
    columns = []
    for token in tokens:
        # Please forgive me god
        if "." in token:
            token = token.split(".")[1]
        if token == "FROM":
            break
        elif token == ",":
            continue
        else:
            columns.append(token)
    return columns

def parse_columns(tokens,table_name):
    results = [ create_column(name,_type,table_name) for name,_type in parse_args_list(tokens) ]
    return results

def parse_query(query):
    results = []
    token = ""
    in_string = False
    quote_mode = False
    for char in query:
        if char in " \n\t" and not in_string:
            if len(token) > 0:
                results.append(token)
                token = ""
            quote_mode = False
        elif char in "'":
            if in_string:
                results.append(token)
                token = ""
                in_string = False
                quote_mode = True 
            elif quote_mode:
                token = results.pop()
                token = token + char
                quote_mode = False
                in_string = True
            else:
                in_string = True
        elif char in "(,);" and not in_string:
            if len(token) > 0:
                results.append(token)
            results.append(char)
            token = "" 
            quote_mode = False
        else:
            token = token + char 
        if quote_mode and len(token) > 0: 
            quote_mode = False
    if len(token) > 0:
        results.append(token)
    return results


class Database:
    """
    Models a database
    filename : The filename for this database
    """
    def __init__(self, filename):
        self.filename = filename
        self.tables = {}

    def has_table(self,table_name):
        """
        returns true if table_name is in the list of tables
        """
        return table_name in self.tables

    def add_table(self,table):
        self.tables[table.name] = table

    def insert_record(self,table_name,record,columns):
        return self.tables[table_name].insert(record,columns)

    def get_records(self,table_name,cols=None,pred=None,order=None):
        return self.tables[table_name].get(cols,pred,order)

class Table:
    """
    Models a table in a database
    name : Name of the table
    columns : A list of column objects
    """
    def __init__(self,name,columns):
        self.name = name
        self.columns = columns
        self.rows = []

    def insert(self,record,columns):
        # Default to all columns
        if columns is None:
            columns = [ column.name for column in self.columns ]
        
        # Make a dictionary to reference each column value by column name
        # Thanks Nahum for showing me this pattern
        labeled_columns = dict(zip(columns,record))

        converted_record = []
        for column in self.columns:
            # get value or NULL if it isn't exist
            value = labeled_columns.get(column.name,"NULL")
            value = column.convert(value)
            converted_record.append(value)

        self.rows.append(tuple(converted_record))
        return True

    def get_selected_columns_index(self,selected_cols):
        index_columns = []
        for col in selected_cols:
            for index,table_column in enumerate(self.columns):
                if col == table_column.name:
                    index_columns.append(index)
        return index_columns

    def _expand_columns(self,columns):
        selected = []
        if cols is None:
            selected = self.rows
        else:
            expanded_cols = []
            for col in cols:
                if "*" in col:
                    expanded_cols = expanded_cols + [column.name for column in self.columns]
                else:
                    expanded_cols.append(col)
            updated_col_order = self.get_selected_columns_index(expanded_cols)
            for row in self.rows:
                result = []
                for col in updated_col_order:
                    result.append(row[col])
                selected.append(tuple(result))
        return selected


    def get(self,cols=None,pred=None,order=None):
        # Rethink order of operations/structure of this.
        # Right now we have to do some weird state tracking to
        # order columns properly if column order changes.
        selected = []
        updated_col_order = None
        selected = self._expand_columns(cols)

        if order is not None:
            order_indexes = []
            for order_index in order:
                for index,col in enumerate(self.columns):
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
        return "{}: ({})".format(self.name,self.columns)
    __repr__ = __str__

class Column:
    """
    Models a column in a table
    name : Name of the column
    type : The type of data that goes in this column
    """
    def __init__(self,name,_type,table_name):
        self.name = name
        self.type = _type
        self.table_name = table_name
        self.full_name = "{}.{}".format(table_name,name)

    def convert(self,arg):
        converted = None
        if arg == "NULL":
            pass
        elif self.type == ColumnType.INTEGER:
            converted = int(arg)
        elif self.type == ColumnType.REAL:
            converted = float(arg)
        elif self.type == ColumnType.TEXT:
            converted = arg
        return converted

    def __str__(self):
        return "{}({})".format(self.full_name,self.type)
    __repr__ = __str__

class ColumnType(Enum):
    NULL = 1
    INTEGER = 2
    REAL = 3
    TEXT = 4

def create_column(name, str_type, table_name):
    type_map = {
            "INTEGER" : ColumnType.INTEGER,
            "REAL" : ColumnType.REAL,
            "TEXT" : ColumnType.TEXT,
        }
    return Column(name, type_map[str_type], table_name)

if __name__ == '__main__':
    conn = connect("test.db")
    conn.execute("CREATE TABLE students (id INTEGER, name TEXT, gpa REAL);")
    conn.execute("INSERT INTO students (id, name) VALUES (1, 'sean'), (2, 'shaun'), (3, 'shawn');")
    conn.execute("CREATE TABLE teachers (id INTEGER, name TEXT, age INTEGER);")
    conn.execute("INSERT INTO teachers (name, id) VALUES ('josh', 1), ('charles', 2), ('bill', 3);")
    result = conn.execute("SELECT name, id FROM students WHERE id > 1 ORDER BY name;")
    print(result)
    result = conn.execute("SELECT * FROM teachers WHERE id < 2 ORDER BY name;")
    print(result)
    result = conn.execute("SELECT students.*, *, col_1 FROM students WHERE id < 2 ORDER BY col_3, col_1;")
    
