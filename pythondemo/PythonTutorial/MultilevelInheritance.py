class Grandparent:
    def __init__(self, city):
        self.__city = city

    def get_city(self):
        return self.__city

    def height(self):
        print("I have inherited my height from my Grandfather.")


class Parent(Grandparent):
    def __init__(self, city, lastname):
        super().__init__(city)
        self.__lastname = lastname

    def get_lastName(self):
        return self.__lastname

    def intelligence(self):
        print("I have inherited my intelligence from my Father.")


class Child(Parent):
    def __init__(self, name, lastname, city):
        Parent.__init__(self, city, lastname)
        self.__name = name

    def experience(self):
        print("My experiences are all my own.")


print(help(Child))
c = Child("Nitin", "Baxi", "Bangalore")
c.height()
c.intelligence()
c.experience()
p1 = Parent("Hazaribag", "Sinha")
print(p1.get_city())
print(p1.get_lastName())


class Person(Parent):
    def __init__(self, city, firstname, lastname):
        Parent.__init__(self, city, lastname)
        self.__firstname = firstname

    def get_firstname(self):
        return self.__firstname

    def get_introduction(self):
        lastname = super().get_lastName()
        city = super().get_city()
        print("Hi I am %s %s and i am from %s" % (self.__firstname, lastname, city))

    def get_information(self):
        lastname = self.get_lastName()
        city = self.get_city()
        print("Hi I am %s %s and i am from %s" % (self.__firstname, lastname, city))


person = Person("Hazaribag", "Nitin", "Baxi")
print(person.get_city())
print(person.get_firstname())
print(person.get_lastName())

p = Person("Bangalore", "Neeraj", "Kumar")
p.get_introduction()
p.get_information()
