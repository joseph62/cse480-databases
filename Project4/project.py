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
            results = self.do_insert(tokens)
        elif tokens[0] == "SELECT":
            results = self.do_select(tokens)
        elif tokens[0] == "DELETE":
            results = self.do_delete(tokens)
        elif tokens[0] == "UPDATE":
            results = self.do_update(tokens)
        else:
            results = "Error"
        return results

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass

    def do_delete(self,tokens):
        table_name = tokens[tokens.index("FROM")+1]
        predicate = parse_predicate(tokens)
        return self.database.delete_record(table_name,predicate)

    def do_update(self,tokens):
        table_name = tokens[tokens.index("UPDATE")+1]
        predicate = parse_predicate(tokens)
        setter_value = parse_set(tokens)
        return self.database.update_records(table_name,setter_value,predicate)


    def do_create(self,tokens):
        tokens = tokens[2:]
        table_name = tokens.pop(0)
        columns = parse_columns(tokens,table_name)
        table = Table(table_name,columns)
        self.database.add_table(table)
        return self.database.has_table(table.name)

    def do_insert(self,tokens):
        tokens = tokens[2:]
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
        print(tokens)
        tokens = tokens[1:]
        columns = parse_selected_columns(tokens)
        distinct_columns = []
        if "DISTINCT" in columns:
            distinct_index = columns.index("DISTINCT") 
            distinct_columns.append(columns[distinct_index+1])
            columns.pop(distinct_index)
        table_name = tokens[tokens.index("FROM")+1]
        predicate = parse_predicate(tokens)
        order = parse_order(tokens)
        return self.database.get_records(table_name, columns, predicate, order, distinct_columns)

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

def parse_order(tokens):
    order = None
    if "ORDER" in tokens:
        order = [ x[0] for x in parse_args_list(tokens[tokens.index("ORDER")+2:-1]) ]
    return order

def parse_predicate(tokens):
    predicate = None
    if "WHERE" in tokens:
        if "ORDER" in tokens:
            predicate_tokens = tokens[tokens.index("WHERE")+1:tokens.index("ORDER")]
            predicate = Predicate(predicate_tokens)
        else:
            predicate_tokens = tokens[tokens.index("WHERE")+1:tokens.index(";")]
            predicate = Predicate(predicate_tokens)
    return predicate 

def parse_set(tokens):
    start_index = tokens.index("SET")+1
    end_index = tokens.index("WHERE") if "WHERE" in tokens else tokens.index(";")
    set_list = tokens[start_index:end_index]
    set_columns = []
    set_column = []
    for token in set_list:
        if token == ",":
            if len(set_column) > 0:
                set_columns.append(set_column)
                set_column = []
        else:
            set_column.append(token)
    set_columns.append(set_column)
    return set_columns

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

    def delete_record(self,table_name,predicate=None):
        return self.tables[table_name].delete(predicate)

    def update_records(self,table_name,setter_columns,predicate=None):
        return self.tables[table_name].set(setter_columns, predicate)

    def get_records(self,table_name,cols=None,pred=None,order=None,distinct_columns=[]):
        return self.tables[table_name].get(cols,pred,order,distinct_columns)

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
            # get value or NULL if it doesn't exist
            value = labeled_columns.get(column.name,"NULL")
            value = column.convert(value)
            converted_record.append(value)

        self.rows.append(Row(self.name,self.columns,tuple(converted_record)))
        return True

    def delete(self,predicate=None):
        result = []
        if predicate is not None:
            for row in self.rows:
                column = row.get_column(predicate.column_name)
                value = row.get_element(predicate.column_name)
                if not predicate.execute(value,column):
                    result.append(row)
        self.rows = result

    def set(self,setter_columns,predicate=None):
        for setter_column in setter_columns:
            for row in self.rows:
                set_column = row.get_column(setter_column[0])
                set_value = set_column.convert(setter_column[-1])
                if predicate is None:
                    row.set_element(set_column.full_name,set_value)
                else:

                    predicate_column = row.get_column(predicate.column_name)
                    predicate_value = row.get_element(predicate.column_name)
                    if predicate.execute(predicate_value,predicate_column):
                        row.set_element(set_column.full_name,set_value)

    def get(self,columns=None,predicate=None,order=None,distinct_columns=[]):
        columns = self._expand_columns(columns)
        result = self.rows
        result = self._order_rows(order,result)
        result = self._filter_rows(predicate,result)
        result = self._distinct_row(distinct_columns,result)
        result = self._select_columns(columns,result)
        return result

    def _get_selected_columns_index(self,selected_cols):
        index_columns = []
        for col in selected_cols:
            for index,table_column in enumerate(self.columns):
                if col == table_column.name:
                    index_columns.append(index)
        return index_columns

    def _expand_columns(self,columns):
        expanded_columns = []
        for column in columns:
            if "*" in column:
                expanded_columns += [column.full_name for column in self.columns]
            else:
                expanded_columns.append(column)
        return expanded_columns

    def _order_rows(self,order,rows):
        if order is None:
            return rows
        order_indexes = []
        for column_name in order:
            order_indexes.append(self._get_column_index(column_name))
        def deref_row(row):
            sort_on = []
            for index in order_indexes:
                sort_on.append(row.get_index(index))
            return sort_on
        rows.sort(key=deref_row)
        return rows

    def _distinct_row(self,distinct_columns,rows):
        result = []
        if len(distinct_columns) == 0:
            return rows
        track = set()
        for distinct_column in distinct_columns:
            for row in rows:
                value = row.get_element(distinct_column)
                if value not in track:
                    track.add(value)
                    result.append(row)
        return result


    def _get_column_index(self,column_name):
        for index,column in enumerate(self.columns):
            if column_name == column.name or column_name == column.full_name:
                return index
        return -1

    def _select_columns(self,columns,rows):
        result = []
        for row in rows:
            result_row = []
            for column in columns:
                result_row.append(row.get_element(column))
            result.append(tuple(result_row))
        return result

    def _filter_rows(self,predicate,rows):
        result = []
        if predicate is None:
            return rows
        for row in rows:
            column = row.get_column(predicate.column_name)
            value = row.get_element(predicate.column_name)
            if predicate.execute(value,column):
                result.append(row)
        return result

    def __str__(self):
        return "{}: ({})".format(self.name,self.columns)
    __repr__ = __str__

class Row:
    def __init__(self,table_names,columns,data):
        self.table_names = table_names
        self.headers = columns
        self.data = data
        self.pairs = dict(zip([column.name for column in columns],self.data))
        self.long_pairs = dict(zip([column.full_name for column in columns],self.data))

    def __str__(self):
        return "{}".format(self.data)
    __repr__ = __str__

    def get_index(self,index):
        return self.data[index]

    def get_element(self,column_name):
        if column_name in self.pairs:
            return self.pairs[column_name]
        elif column_name in self.long_pairs:
            return self.long_pairs[column_name]
        return None
    
    def set_element(self,column_name,value):
        for index,(column,_) in enumerate(zip(self.headers,self.data)):
            if column.name == column_name or column.full_name == column_name:
                self.data = list(self.data)
                self.data[index] = value 
                self.data = tuple(self.data)
                self.pairs[column.name] = value
                self.long_pairs[column.full_name] = value

    def get_column(self,column_name):
        for column in self.headers:
            if column_name == column.name or column_name == column.full_name:
                return column
        return None

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

class Predicate:
    def __init__(self,tokens):
        assert len(tokens) >= 3
        self.tokens = tokens
        self.column_name = tokens[0]
        self.value = tokens[-1]
        self.operator = " ".join(tokens[1:-1])

    def execute(self,value,column):
        compare = column.convert(self.value)
        if self.operator == "IS":
            return value is None
        elif self.operator == "IS NOT":
            return value is not None
        elif self.operator == ">":
            return value is not None and value > compare
        elif self.operator == "<":
            return value is not None and value < compare
        elif self.operator == "=":
            return value is not None and value == compare
        elif self.operator == "!=":
            return value is not None and value != compare
        return False

    def __str__(self):
        return "{} {} {}".format(self.column_name, self.operator, self.value)
    __repr__ = __str__
    

if __name__ == '__main__':
    conn = connect("test.db")

    conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, class TEXT);")
    conn.execute("CREATE TABLE classes (course TEXT, instructor TEXT);")

    conn.execute("INSERT INTO students VALUES ('Josh', 99, 'CSE480'), ('Dennis', 99, 'CSE480'), ('Jie', 52, 'CSE491');")
    conn.execute("INSERT INTO students VALUES ('Cam', 56, 'CSE480'), ('Zizhen', 56, 'CSE491'), ('Emily', 74, 'CSE431');")

    conn.execute("INSERT INTO classes VALUES ('CSE480', 'Dr. Nahum'), ('CSE491', 'Dr. Josh'), ('CSE431', 'Dr. Ofria');")

    conn.execute("SELECT students.name, students.grade, classes.course, classes.instructor FROM students LEFT OUTER JOIN classes ON students.class = classes.course ORDER BY classes.instructor, students.name, students.grade;")

    #conn.execute("CREATE TABLE students (name TEXT, grade INTEGER, notes TEXT);")
    #conn.execute("INSERT INTO students VALUES ('Josh', 99, 'Likes Python'), ('Dennis', 99, 'Likes Networks'), ('Jie', 52, 'Likes Driving');")
    #conn.execute("INSERT INTO students VALUES ('Cam', 56, 'Likes Anime'), ('Zizhen', 56, 'Likes Reading'), ('Emily', 74, 'Likes Environmentalism');")
    #
    #print(conn.execute("SELECT * FROM students ORDER BY name;"))
    #print(conn.execute("SELECT DISTINCT grade FROM students ORDER BY grade;"))
    #print(conn.execute("SELECT DISTINCT grade FROM students WHERE name < 'Emily' ORDER BY name;"))

    #conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
    #conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
    #conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Skyrim');")
    #result = conn.execute("SELECT * FROM table WHERE two > 50 ORDER BY three, two, one;")
    #print(result)
    #conn.execute("DELETE FROM table WHERE two > 50 ;")
    #result = conn.execute("SELECT * FROM table ORDER BY three, two, one;")
    #print(result)
    #conn.execute("UPDATE table SET table.one = 1.0;")
    #conn.execute("UPDATE table SET table.one = 2.0, two = 100 WHERE two > 40;")
    #result = conn.execute("SELECT * FROM table ORDER BY three, two, one;")
    #print(result)


    #conn.execute("CREATE TABLE students (id INTEGER, name TEXT, gpa REAL);")
    #conn.execute("INSERT INTO students (id, name) VALUES (1, 'sean'), (2, 'shaun'), (3, 'shawn');")
    #conn.execute("CREATE TABLE teachers (id INTEGER, name TEXT, age INTEGER);")
    #conn.execute("INSERT INTO teachers (name, id) VALUES ('josh', 1), ('charles', 2), ('bill', 3);")
    #result = conn.execute("SELECT name, id FROM students WHERE id > 1 ORDER BY name;")
    #print(result)
    #result = conn.execute("SELECT *, *, teacher.* FROM teachers WHERE name IS NOT NULL ORDER BY name;")
    #print(result)
    #result = conn.execute("SELECT students.name, id FROM students WHERE id < 2 ORDER BY name, id;")
    #print(result)
    
