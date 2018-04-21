#! /usr/bin/env python3
"""
Simple Document Store
"""

from functools import partial

def find_matching(collection,predicate):
    results = collection
    def contains(key,value,document):
        return key in document and value == document[key]
    for key,value in predicate.items():
        results = list(filter(partial(contains,key,value),results))
    return results

class Collection:
    """
    A list of dictionaries (documents) accessible in a DB-like way.
    """

    def __init__(self):
        """
        Initialize an empty collection.
        """
        self.store = []

    def insert(self, document):
        """
        Add a new document (a.k.a. python dict) to the collection.
        """
        self.store.append(document) 

    def find_all(self):
        """
        Return list of all docs in database.
        """
        return self.store

    def delete_all(self):
        """
        Truncate the collection.
        """
        self.store = []

    def find_one(self, where_dict):
        """
        Return the first matching doc.
        If none is found, return None.
        """
        results = self.find(where_dict)
        return None if len(results) == 0 else results[0]

    def find(self, where_dict):
        """
        Return matching list of matching doc(s).
        """
        return find_matching(self.store,where_dict)

    def count(self, where_dict):
        """
        Return the number of matching docs.
        """
        return len(self.find(where_dict))

    def delete(self, where_dict):
        """
        Delete matching doc(s) from the collection.
        """
        remove = self.find(where_dict)
        self.store = list(filter(lambda ds: ds not in remove,self.store))


    def update(self, where_dict, changes_dict):
        """
        Update matching doc(s) with the values provided.
        """
        [ ds.update(changes_dict) for ds in find_matching(self.store,where_dict) ]

    def map_reduce(self, map_function, reduce_function):
        """
        Applies a map_function to each document, collating the results.
        Then applies a reduce function to the set, returning the result.
        """
        result = self.store.copy()
        return reduce_function(map(map_function,result))


class Database:
    """
    Dictionary-like object containing one or more named collections.
    """

    def __init__(self, filename):
        """
        Initialize the underlying database. If filename contains data, load it.
        """
        self.collections = []
        self.filename = filename

    def get_collection(self, name):
        """
        Create a collection (if new) in the DB and return it.
        """
        if name not in [ name_ for name_,coll in self.collections ]:
            self.collections.append((name,Collection()))
        (name,result), *rest = filter(lambda x: x[0] == name,self.collections)
        return result


    def drop_collection(self, name):
        """
        Drop the specified collection from the database.
        """
        self.collections = list(filter(lambda x: x[0] != name,self.collections))

    def get_names_of_collections(self):
        """
        Return a list of the sorted names of the collections in the database.
        """
        return list(map(lambda x: x[0],self.collections))

    def close(self):
        """
        Save and close file.
        """
        pass

if __name__ == "__main__":

    c = Collection()



    docs = [ 
      {"First": "Josh", "Last":"Nahum"}, 
      {"First": "Emily", "Last":"Dolson"}, 
      {"First": "RaceTrack", "Last": "Nahum"}, 
      {"First": "CrashDown", "Last": "Dolson"}, 
    ] 

    for doc in docs: 
      c.insert(doc) 

    assert c.find_all() == docs 

    expected = [ 
      {"First": "Josh", "Last":"Nahum"}, 
      {"First": "RaceTrack", "Last": "Nahum"}, 
    ] 
    assert c.find({"Last": "Nahum"}) == expected
