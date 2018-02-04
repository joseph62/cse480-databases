"""
Name: Sean Joseph
Time to completion:
Comments:

Sources:
"""


def sum_whitespace_separated_ints(input_text):
    """
    Takes string that has ints and whitespace.
    Should return the sum of all the ints (if there are no ints, return 0).

    Example use:
    input_text = " 54 2   32 "
    count = sum_whitespace_separated_ints(input_text) # count should be 88
    """
    nums = [ int(i) for i in input_text.split(" ") if len(i) > 0 ] 
    result = 0
    for num in nums:
        result += num
    return result

if __name__ == "__main__":
    print(sum_whitespace_separated_ints("1  3 134 324   32    "))
