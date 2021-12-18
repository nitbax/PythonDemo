class PythonAdvanced:
    def __init__(self, name, emp_id, post, level):
        self.__name = name
        self.__emp_id = emp_id
        self.__post = post
        self.__level = level

    def __repr__(self):
        return "Employee details are as follows :- ('{}', {} , '{}', {})".format(
            self.__name, self.__emp_id, self.__post, self.__level
        )

    def __str__(self):
        return "('{}', {} , '{}', {})".format(
            self.__name, self.__emp_id, self.__post, self.__level
        )


print(PythonAdvanced.__dict__)
pa = PythonAdvanced("Nitin", 10891400, "TL", 9)
print(pa)
print(repr(pa))
print(str(pa))
print(pa.__repr__())
print(pa.__str__())
print(1 + 2)
print(int.__add__(1, 2))
print("a" + "b")
print(str.__add__("a", "b"))
print(3 - 1)
print(int.__sub__(3, 1))
print(3 * 1)
print(int.__mul__(3, 1))
print(3.0 * 1.5)
print(float.__mul__(3.0, 1.5))


class Savings:
    def __init__(self, amount):
        self.__amount = amount

    def __add__(self, other):
        return self.__amount + other.__amount

    def __sub__(self, other):
        return self.__amount - other.__amount

    def __mul__(self, other):
        if type(other) == int or type(other) == float:
            return self.__amount * other
        else:
            raise ValueError("Can only multiply int or float")


s1 = Savings(12000)
s2 = Savings(10000)
print(s1 + s2)
print(s1 - s2)
print(s1 * 2)
print(s1 * 2.5)
