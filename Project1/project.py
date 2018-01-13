"""
Project # 1 
Name: Sean Joseph
Time to completion:
Comments:
    I tried to construct the xml without constructing the tree at first,
    but I think I ran into xml encoding issues. The solution for the write
    function didn't change too much as a result though. I just swapped out
    the way I was creating it. I suppose walking structured data like records
    is not any different for any data style.

Sources:
    https://docs.python.org/3.6/library/index.html
        Python documentation for csv,json, and ElementTree/xml
"""
import csv
import json
import xml.etree.ElementTree

# You are welcome to add code outside of functions
# like imports and other statements

def read_csv_string(input_):
    """
    Takes a string which is the contents of a CSV file.
    Returns an object containing the data from the file.
    The specific representation of the object is up to you.
    The data object will be passed to the write_*_string functions.
    """
    records = []
    input_ = input_.split("\n")
    keys = input_[0].split(',')
    reader = csv.DictReader(input_)
    for row in reader:
        records.append(row)
    return (keys,records)


def write_csv_string(data):
    """
    Takes a data object (created by one of the read_*_string functions).
    Returns a string in the CSV format.
    """
    result = []
    keys = data[0]
    data = data[1]
    result.append(",".join(keys))
    for record in data:
        values = []
        for key in keys:
            values.append(record[key])
        result.append(",".join(values))
    return "\n".join(result) + "\n"


def read_json_string(input_):
    """
    Similar to read_csv_string, except works for JSON files.
    """
    data = json.loads(input_)
    keys = list(data[0].keys())
    return (keys,data)


def write_json_string(data):
    """
    Writes JSON strings. Similar to write_csv_string.
    """
    return json.dumps(data[1])


def read_xml_string(input_):
    """
    You should know the drill by now...
    """
    root = xml.etree.ElementTree.fromstring(input_)
    data = []
    keys = []
    for record in root:
        result = {}
        for item in record:
            key = item.tag
            element = item.text
            if key not in keys:
                keys.append(key)
            result[key] = element
        data.append(result)
    return (keys,data)


def write_xml_string(data):
    """
    Feel free to write what you want here.
    """
    keys = data[0]
    data = data[1]
    result = []
    tree = xml.etree.ElementTree.Element("data")
    for record in data:
        result = xml.etree.ElementTree.SubElement(tree,"record")
        for key in keys:
            value = xml.etree.ElementTree.SubElement(result,key)
            value.text = record[key]
    return str(xml.etree.ElementTree.tostring(tree,encoding="utf-8"), encoding="utf-8")

# The code below isn't needed, but may be helpful in testing your code.
if __name__ == "__main__":
    input_ = """
    [{"col1": "1", "col2": "2", "col3": "3"}, {"col1": "4", "col2": "5", "col3": "6"}]
    """
    expected = """
    [{"col1": "1", "col2": "2", "col3": "3"}, {"col1": "4", "col2": "5", "col3": "6"}]
    """

    def super_strip(input_):
        """
        Removes all leading/trailing whitespace and blank lines
        """
        lines = []
        for line in input_.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)
        return "\n".join(lines) + "\n"

    input_ = super_strip(input_)
    expected = super_strip(expected)

    print("Input:")
    print(input_)
    print()
    data = read_json_string(input_)
    print("Your data object:")
    print(data)
    print()
    output = write_csv_string(data)
    output = super_strip(output)
    print("Output:")
    print(output)
    print()
