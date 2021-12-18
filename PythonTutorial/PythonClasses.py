import datetime


class PythonClasses:
    def print_statement(self):
        print("My name is Anthony Gonsalves")


d3 = datetime.datetime.strptime("2020-03-01", "%Y-%m-%d")
print(d3)

ob1 = PythonClasses()
print(ob1)
ob1.print_statement()
ob1.nitin = "Kumar"
print(ob1.nitin)


class Student:
    name = ""
    score = 0
    active = True

    def __init__(self):
        print("Initialize called")


s1 = Student()
print(s1.score)


class Employee:
    middle_name = "Kumar"
    employee_list = []

    def __init__(self, first, last):
        self.first = first
        self.last = last
        self.mail = first + "." + last + "@xyz.com"

    def full_name(self):
        return "{} {} {}".format(self.first, self.middle_name, self.last)

    def upper_case(self):
        self.first = self.first.upper()
        Employee.middle_name = Employee.middle_name.upper()
        self.last = self.last.upper()


e1 = Employee("Nitin", "Baxi")
print(e1.full_name())
print(e1.mail)
print(e1.middle_name)
print(Employee.full_name(e1))
e1.upper_case()
print(e1.full_name())
print(e1.__dict__)
print(Employee.__dict__)
print(Employee.employee_list)
e1.employee_list.extend(["Nitin", "Namit", "Neelabh"])
print(e1.employee_list)
print(Employee.employee_list)
