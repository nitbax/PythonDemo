import math


class Shape:
    def __init__(self, shape_type, color="Red"):
        self.__type = shape_type
        self.__color = color

    def get_type(self):
        return self.__type

    def get_color(self):
        return self.__color

    def get_area(self):
        pass

    def get_perimeter(self):
        pass


circle = Shape("circle")
square = Shape("square", "Blue")
print(circle.get_type(), circle.get_color())
print(square.get_type(), square.get_color())


class Circle(Shape):
    def __init__(self, radius, color="Green"):
        Shape.__init__(self, "circle", color)
        self.__radius = radius

    def get_area(self):
        return math.pi * self.__radius * self.__radius

    def get_perimeter(self):
        return math.pi * self.__radius * 2


class Square(Shape):
    def __init__(self, side, color="Blue"):
        super().__init__("square", color)
        self.__side = side

    def get_area(self):
        return self.__side * self.__side

    def get_perimeter(self):
        return 4 * self.__side


ci = Circle(10)
print(ci.get_type())
print(ci.get_color())
print(ci.get_area())
print(ci.get_perimeter())

squ = Square(10)
print(squ.get_type())
print(squ.get_color())
print(squ.get_area())
print(squ.get_perimeter())

print(help(Shape))
print(help(Circle))
