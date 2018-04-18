#! /usr/bin/env python3
"""
Name: Sean Joseph
Time To Completion: 8 hours
Comments:
    This project was fairly difficult. Not because
    the spec was overly demanding, but because design 
    up until now has been lacking. My parsing system
    should have been more separated from the execution
    of the query statements. Perhaps having a separate
    function or class that would go through and configure
    a query object to be interpreted by my connection/database.
Sources:
    Python Docs
    Python Docs Sorting How To
"""
import string, functools
from enum import Enum 
from copy import deepcopy
from pprint import pprint

_LOCKS = {}
_ALL_DATABASES = {}
_CONNECTIONS = []
_COLLATION = {}

def update_connections(db):
    global _CONNECTIONS
    global _ALL_DATABASES
    for connection in _CONNECTIONS:
        if connection.filename == db.filename and not connection.in_transaction:
            connection.database = db
    _ALL_DATABASES[db.filename] = db

def grant_shared_lock(conn):
    global _LOCKS
    filename = conn.filename
    if filename not in _LOCKS:
        _LOCKS[filename] = []
    for lock_conn,lock_type in _LOCKS[filename]:
        if lock_type == Lock.Exclusive and conn != lock_conn:
            return False
    _LOCKS[filename].append((conn,Lock.Shared))
    return True

def grant_reserved_lock(conn):
    global _LOCKS
    filename = conn.filename
    if filename not in _LOCKS:
        _LOCKS[filename] = []
    for lock_conn,lock_type in _LOCKS[filename]:
        if ((lock_type == Lock.Exclusive or lock_type == Lock.Reserved)
                and lock_conn != conn):
            return False
    _LOCKS[filename].append((conn,Lock.Reserved))
    return True

def grant_exclusive_lock(conn):
    global _LOCKS
    filename = conn.filename
    if filename not in _LOCKS:
        _LOCKS[filename] = []
    for lock_conn,lock_type in _LOCKS[filename]:
        if lock_conn != conn:
            return False
    _LOCKS[filename].append((conn,Lock.Exclusive))
    return True

def release_locks(conn):
    global _LOCKS
    filename = conn.filename
    if filename not in _LOCKS:
        _LOCKS[filename] = []
    _LOCKS[filename] = [ lock for lock in _LOCKS[filename] if lock[0] != conn ]

class Lock(Enum):
    Shared = 0
    Reserved = 1
    Exclusive = 2

class Connection:
    def __init__(self, filename, timeout, isolation_level):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self.filename = filename
        self.timeout = timeout
        self.isolation_level = isolation_level
        self.in_transaction = False
        #self.transaction_queries = []
        global _ALL_DATABASES
        global _CONNECTIONS
        if filename not in _ALL_DATABASES:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database
        else:
            self.database = _ALL_DATABASES[filename]
        _CONNECTIONS.append(self)

    def create_collation(self,name,func):
        global _COLLATION
        assert name not in _COLLATION
        _COLLATION[name] = func

    def executemany(self, statement, params):
        def replace_params(statement,params):
            result = statement
            for elem in params:
                result = result.replace("?",str(elem),1)
            return result

        for param in params:
            paramed_statement = replace_params(statement,param)
            self.execute(paramed_statement)
        
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
        elif tokens[0] == "CREATE" and tokens[1] == "VIEW":
            results = self.do_view(tokens)
        elif tokens[0] == "INSERT" and tokens[1] == "INTO":
            assert grant_reserved_lock(self)
            results = self.do_insert(tokens)
        elif tokens[0] == "BEGIN":
            results = self.begin_transaction(tokens)
        elif tokens[0] == "COMMIT":
            if grant_exclusive_lock(self):
                results = self.commit_transaction(tokens)
            else:
                results = self.rollback_transaction(tokens)
        elif tokens[0] == "ROLLBACK":
            results = self.rollback_transaction(tokens)
        elif tokens[0] == "SELECT":
            assert grant_shared_lock(self)
            results = self.do_select(tokens)
        elif tokens[0] == "DELETE":
            assert grant_reserved_lock(self)
            results = self.do_delete(tokens)
        elif tokens[0] == "UPDATE":
            assert grant_reserved_lock(self)
            results = self.do_update(tokens)
        elif tokens[0] == "DROP":
            results = self.do_drop(tokens)
        else:
            results = "Error"
        if not self.in_transaction:
            release_locks(self)
        return results

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass

    def begin_transaction(self,tokens):
        if self.in_transaction:
            raise Exception("Connection already in transaction!")
        if tokens[1] == "EXCLUSIVE":
            assert grant_exclusive_lock(self)
        elif tokens[1] == "IMMEDIATE":
            assert grant_reserved_lock(self)

        self.in_transaction = True
        self.database = deepcopy(self.database)

    def commit_transaction(self,tokens):
        if not self.in_transaction:
            raise Exception("Connection not in transaction!")
        self.in_transaction = False
        release_locks(self)
        update_connections(self.database)

    def rollback_transaction(self,tokens):
        if not self.in_transaction:
            raise Exception("Nothing to rollback!")
        self.in_transaction = False
        release_locks(self)
        self.database = _ALL_DATABASES[self.filename] 

    def do_delete(self,tokens):
        table_name = tokens[tokens.index("FROM")+1]
        predicate = parse_predicate(tokens)
        return self.database.delete_record(table_name,predicate)

    def do_update(self,tokens):
        table_name = tokens[tokens.index("UPDATE")+1]
        predicate = parse_predicate(tokens)
        setter_value = parse_set(tokens)
        return self.database.update_records(table_name,setter_value,predicate)

    def do_drop(self,tokens):
        tokens = tokens[2:]
        raise_if_not_exist = True
        if tokens[0] == "IF":
            tokens = tokens[2:]
            raise_if_not_exist = False
        table_name = tokens[0]
        if raise_if_not_exist:
            self.database.remove_table(table_name)
        else: 
            self.database.remove_table_if_exist(table_name)

    def do_view(self,tokens):
        tokens = tokens[2:]
        view_name = tokens.pop(0)
        tokens = tokens[1:]
        view = View(view_name,self,tokens)
        self.database.add_view(view)
        #table = Table(table_name,columns)
        #if raise_if_exist:
        #    self.database.add_table(table)
        #else:
        #    self.database.add_table_if_not_exist(table)
        #return self.database.has_table(table.name)

    def do_create(self,tokens):
        tokens = tokens[2:]
        raise_if_exist = True
        if tokens[0] == "IF":
            tokens = tokens[3:]
            raise_if_exist = False
        table_name = tokens.pop(0)
        columns = parse_columns(tokens,table_name)
        table = Table(table_name,columns)
        if raise_if_exist:
            self.database.add_table(table)
        else:
            self.database.add_table_if_not_exist(table)
        return self.database.has_table(table.name)

    def do_insert(self,tokens):
        tokens = tokens[2:]
        table_name = tokens.pop(0)
        columns = None
        default = "DEFAULT" in tokens
        if tokens[0] == "(":
            columns = [ column[0] for column in parse_args_list(tokens) ]
        tokens = tokens[tokens.index("VALUES")+1:]
        records = parse_records(tokens)
        if len(records) == 0 and default:
            self.database.insert_default_record(table_name)
        else: 
            for record in records:
                values = [ arg[0] for arg in parse_args_list(record) ] 
                self.database.insert_record(table_name,values,columns) 

    def do_select(self,tokens):
        tokens = tokens[1:]
        columns = parse_selected_columns(tokens)
        def consolidate_aggregates(columns):
            aggregates = {}
            result_columns = []
            while len(columns) > 0:
                column = columns.pop(0)
                if column in ["min","max"]:
                    func = column
                    columns.pop(0)
                    column = columns.pop(0)
                    columns.pop(0)
                    aggregates[column] = func
                    result_columns.append(column)
                else:
                    result_columns.append(column)
            return (result_columns,aggregates) 
        columns,aggregates = consolidate_aggregates(columns)
        distinct_columns = []
        if "DISTINCT" in columns:
            distinct_index = columns.index("DISTINCT") 
            distinct_columns.append(columns[distinct_index+1])
            columns.pop(distinct_index)
        table_name = tokens[tokens.index("FROM")+1]
        predicate = parse_predicate(tokens)
        order = parse_order(tokens)
        if "LEFT" in tokens:
            join_table_name = tokens[tokens.index("JOIN")+1]
            assert self.database.has_table(join_table_name)
            assert "ON" in tokens
            join_table = self.database.get_table(join_table_name)
            table = self.database.get_table(table_name)
            combined_columns = table.columns + join_table.columns
            on_clause = parse_on_clause(tokens)
            temp_table = Table("temp_join_table",combined_columns)
            left_table_rows = table.rows
            right_table_rows = join_table.rows
            temp_table_rows = []
            for left_row in left_table_rows:
                left_element = left_row.get_element(on_clause.left_table_column)
                left_column = left_row.get_column(on_clause.left_table_column)
                result_right_row = []
                for right_row in right_table_rows:
                    right_element = right_row.get_element(on_clause.right_table_column)
                    right_column = right_row.get_column(on_clause.right_table_column)
                    result_right_row = [None] * len(right_row.data)
                    if on_clause.execute(left_element,left_column,right_element,right_column):
                        result_right_row = right_row.data
                        break
                combined_row = tuple(list(left_row.data) + list(result_right_row))
                temp_table_rows.append(Row(temp_table.name,combined_columns,combined_row))
            temp_table.rows = temp_table_rows
            return temp_table.get(columns,predicate,order,distinct_columns,aggregates)
        selected_rows = self.database.get_records(table_name, columns,
                                                    predicate, order, 
                                                    distinct_columns,
                                                    aggregates)
        return selected_rows

def connect(filename,timeout=0.1,isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename,timeout,isolation_level)

def parse_on_clause(tokens):
    assert "ON" in tokens
    clause = tokens[tokens.index("ON")+1:tokens.index("ON")+4]
    return Clause(clause)


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
        order_list = parse_args_list(tokens[tokens.index("ORDER")+2:-1])
        for order in order_list:
            func = None
            if "COLLATE" in order:
                func_key = order.pop(order.index("COLLATE")+1)
                order.pop(order.index("COLLATE"))
                global _COLLATION
                func = _COLLATION[func_key]
            length = len(order)
            assert length < 5 and length > 0
            if length == 1:
                order.append(False)
            else:
                order[1] = order[1] == "DESC"
            order.append(func)
        order = order_list 
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
    args = parse_args_list(tokens)
    columns = []
    for arg in args:
        if len(arg) == 4:
            arg[2] = table_name
        elif len(arg) == 2:
            arg.append(table_name)
        columns.append(create_column(*arg))
    return columns

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
        self.views = {}

    def __deepcopy__(self,memo):
        db = Database(self.filename)
        for table in self.tables.values():
            db.add_table(deepcopy(table))
        return db

    def has_table(self,table_name):
        """
        returns true if table_name is in the list of tables
        """
        return table_name in self.tables or table_name in self.views

    def get_table(self,table_name):
        if table_name in self.views:
            return self.views[table_name]
        else:
            return self.tables[table_name]

    def remove_table(self,table_name):
        if table_name not in self.tables:
            raise Exception("Table '{}' doesn't exist!".format(table_name))
        del self.tables[table_name]

    def remove_table_if_exist(self,table_name):
        if table_name in self.tables:
            del self.tables[table_name] 

    def add_view(self,view):
        self.views[view.name] = view

    def add_table(self,table):
        if table.name in self.tables:
            raise Exception("Table '{}' already exists!".format(table.name))
        self.tables[table.name] = table

    def add_table_if_not_exist(self,table):
        if table.name not in self.tables:
            self.tables[table.name] = table

    def insert_record(self,table_name,record,columns):
        return self.tables[table_name].insert(record,columns)

    def insert_default_record(self,table_name):
        return self.tables[table_name].insert_default()

    def delete_record(self,table_name,predicate=None):
        return self.tables[table_name].delete(predicate)

    def update_records(self,table_name,setter_columns,predicate=None):
        return self.tables[table_name].set(setter_columns, predicate)

    def get_records(self,table_name,cols=None,pred=None,order=None,distinct_columns=[],aggregates={}):
        if table_name in self.views:
            return self.views[table_name].get(cols,pred,order,distinct_columns)
        else:
            return self.tables[table_name].get(cols,pred,order,distinct_columns,aggregates)

class View:
    def __init__(self,name,connection,query):
        self.name = name
        self.conn = connection
        self.query = query
        self.view_columns = parse_selected_columns(self.query[1:])

        self.table_names = [query[query.index("FROM")+1]]
        if "LEFT" in query:
            self.table_names.append(query[query.index("JOIN")+1])
        
    def _expand_columns(self,columns,all_columns):
        expanded_columns = []
        for column in columns:
            if "*" in column:
                expanded_columns += [column.full_name for column in all_columns]
            else:
                expanded_columns.append(column)
        return expanded_columns

    def get(self,columns=None,predicate=None,order=None,distinct_columns=[]):
        rows = self.conn.do_select(self.query)
        viewed_table_columns = []
        for table_name in self.table_names:
            viewed_table = self.conn.database.get_table(table_name) 
            viewed_table_columns.extend(viewed_table.columns) 
        view_columns = self._expand_columns(self.view_columns,viewed_table_columns)
        view_columns = [x if "." not in x else x.split(".")[1] for x in view_columns]
        result_columns = []
        for column in viewed_table_columns:
            name = column.name
            full_name = column.full_name
            if name in view_columns or full_name in view_columns:
                new_column = Column(name,column.type,self.name,column.default)
                result_columns.append(new_column)
        temp_table = Table(self.name,result_columns)
        for row in rows:
            temp_table.insert(row,view_columns)
        return temp_table.get(columns,predicate,order,distinct_columns)

        

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

    def __deepcopy__(self,memo):
        table = Table(self.name,self.columns)
        for row in self.rows:
            table.rows.append(deepcopy(row))
        return table

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
            value = labeled_columns.get(column.name,column.default)
            value = column.convert(value)
            converted_record.append(value)

        self.rows.append(Row(self.name,self.columns,tuple(converted_record)))
        return True

    def insert_default(self):
        record = []
        for column in self.columns:
            record.append(column.default)
        self.rows.append(Row(self.name,self.columns,tuple(record)))

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

    def get(self,columns=None,predicate=None,order=None,distinct_columns=[],aggregates={}):
        columns = self._expand_columns(columns)
        result = self.rows
        result = self._order_rows(order,result)
        result = self._filter_rows(predicate,result)
        result = self._distinct_row(distinct_columns,result)
        result = self._select_columns(columns,result)
        result = self._execute_aggregates(aggregates,result,columns)
        return result

    def _execute_aggregates(self,aggregates,rows,columns):
        if len(aggregates) == 0:
            return rows
        result_row = []
        for column_name,func in aggregates.items():
            result_value = None
            for row in rows:
                value = row[columns.index(column_name)]
                if result_value is None:
                    result_value = value
                elif func == "max":
                    result_value = result_value if value < result_value else value
                elif func == "min":
                    result_value = result_value if value > result_value else value
            result_row.append(result_value)
        return [tuple(result_row)]

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
        for column_name,reverse,coll in reversed(order):
            index = self._get_column_index(column_name)
            if coll is None:
                rows = sorted(rows,
                              key=lambda row: row.get_element(column_name),
                              reverse=reverse)
            else:
                #https://docs.python.org/3/howto/sorting.html#sortinghowto 
                def collated(row):
                    value = row.get_element(column_name)
                    return functools.cmp_to_key(coll)(value)
                rows = sorted(rows,
                              key=collated,
                              reverse=reverse)
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

    def __deepcopy__(self,memo):
        row = Row(self.table_names,self.headers,self.data)
        return row 

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
    def __init__(self,name,_type,table_name,default=None):
        self.name = name
        self.type = _type
        self.table_name = table_name
        self.default= None if default is None else self.convert(default)
        self.full_name = "{}.{}".format(table_name,name)

    def convert(self,arg):
        converted = None
        if arg == "NULL" or arg is None:
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

def create_column(name, str_type, table_name, default=None):
    type_map = {
            "INTEGER" : ColumnType.INTEGER,
            "REAL" : ColumnType.REAL,
            "TEXT" : ColumnType.TEXT,
        }
    return Column(name, type_map[str_type], table_name, default)

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
    

class Clause:
    def __init__(self,tokens):
        assert len(tokens) >= 3
        self.tokens = tokens
        self.left_table_column = tokens[0]
        self.right_table_column = tokens[-1]
        self.operator = " ".join(tokens[1:-1])

    def execute(self,left,left_column,right,right_column):
        left_compare = left_column.convert(left)
        right_compare = right_column.convert(right)
        if left_compare is None or right_compare is None:
            return False
        if self.operator == ">":
            return left_compare > right_compare
        elif self.operator == "<":
            return left_compare < right_compare
        elif self.operator == "=":
            return left_compare == right_compare
        elif self.operator == "!=":
            return left_compare != right_compare
        return False

    def __str__(self):
        return "{} {} {}".format(self.left_table_column, self.operator, self.right_table_column)
    __repr__ = __str__


if __name__ == '__main__':

    def check(sql_statement, conn, expected):
        print("SQL: " + sql_statement)
        result = conn.execute(sql_statement)
        result_list = list(result)
        
        print("expected:")
        pprint(expected)
        print("student: ")
        pprint(result_list)
        assert expected == result_list

    conn = connect("test.db")
    conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
    conn.executemany("INSERT INTO students VALUES (?, ?, ?);", 
        [('Josh', 3.5, 480), 
        ('Tyler', 2.5, 480), 
        ('Tosh', 4.5, 450), 
        ('Losh', 3.2, 450), 
        ('Grant', 3.3, 480), 
        ('Emily', 2.25, 450), 
        ('James', 2.25, 450)])
    check("SELECT max(grade) class FROM students ORDER BY grade;",
    conn,
    [(4.5,)]
    )

    check("SELECT min(class), max(name) FROM students ORDER BY grade, name;",
    conn,
    [(450, 'Tyler')]
    )
