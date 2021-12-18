class Father:
    def height(self):
        print("I have inherited my height from my Father")


class Mother:
    def intelligence(self):
        print("I have inherited my intelligence from my Mother")


class child1(Father, Mother):
    pass


class child2(Mother, Father):
    pass


print(help(child1))
print(help(child2))


class Child(Father, Mother):
    def my_experience(self):
        print("My experience is all my own.")


c = Child()
c.height()
c.intelligence()
c.my_experience()


class Employee:
    def __init__(self, name, age):
        self.__name = name
        self.__age = age

    def show_name(self):
        return self.__name

    def show_age(self):
        return self.__age


class Salary:
    def __init__(self, salary):
        self.__salary = salary

    def get_salary(self):
        return self.__salary


class Database(Employee, Salary):
    def __init__(self, name, age, salary):
        Employee.__init__(self, name, age)
        Salary.__init__(self, salary)


emp1 = Database("Nitin", 28, 20000)
print(emp1.show_name())
print(emp1.show_age())
print(emp1.get_salary())
print(help(Database))
