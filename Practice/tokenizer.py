# Tokenize function.
def tokenize_query(query):
    query = list(query)
    result = []
    token = ""
    in_string = False
    while len(query) > 0:
        char = query.pop(0)
        if char.is_whitespace() and not in_string:
            if len(token) > 0:
                result.append(token)
                token = ""
        else:
            pass
    return result

if __name__ == '__main__':
    query = "CREATE TABLE test VALUES (id INTEGER, name TEXT);"
    print("Query: {}".format(query))
    result = tokenize_query(query)
    print(result)
