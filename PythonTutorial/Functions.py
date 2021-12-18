import math
import random


def helloWorld():
    print("Hello World")


helloWorld()


def helloPerson(name):
    print("Hello", name)


helloPerson("SuperMan")


def sumOfNumbers(a, b):
    print("Print the sum of numbers : ", a + b)


sumOfNumbers(3287, 5342)


def updateList(l1):
    l1.append(50)
    l1.append(60)
    print("Printing list inside function : ", l1)


l1 = [10, 20, 30, 40]
updateList(l1)
print("Printing list outside function : ", l1)


def updateString(str1):
    str1 = str1 + " how are you?"
    print("Printing string inside function : ", str1)


str1 = "Hello Nitin"
updateString(str1)
print("Printing string outside function : ", str1)


def helloProgramming(lang):
    mess = "Hello " + lang
    return mess


print(helloProgramming("Python"))


def calSimpleInterest(p, r, t):
    interest = p * r * t / 100
    return interest


print("Simple interest = ", calSimpleInterest(50000, 8.9, 5))


def keyWordArguments(name, mess):
    print("Print the name and the message :", mess, name)


keyWordArguments(name="Nitin", mess="Good morning")
keyWordArguments(mess="Good evening", name="Nitin")


def mixUpArguments(first, last, greet):
    print(greet, first, last)


mixUpArguments("Nitin", last="Kumar", greet="Hi")


def showEmployeeDesig(desig, name="Employee"):
    print(name, "is of level", desig)


showEmployeeDesig("9")


def multiParamFunction(*levels):
    for i in levels:
        print(i)


multiParamFunction(3, 6, 7, 8, 9, 11, 12)


def calc(*argums):
    sum2 = 0
    for i in argums:
        sum2 = sum2 + i
    print("Value of sum inside function :", sum2)


sum2 = 0
calc(32, 2, 53)
print("Value of sum outside function :", sum2)

# Lambda functions
x = lambda a: a + 10
print("First lambda function :", x(25))
y = lambda a, b: a + b
print("Lambda function with multiple variables:", y(5, 23))


def table(n):
    return lambda a: a * n


tableOf = 51
z = table(tableOf)
for i in range(1, 11):
    print(tableOf, "X", i, "=", z(i))
l2 = {1, 2, 3, 4, 10, 123, 22}
oddList = list(filter(lambda x: (x % 3 == 0), l2))
print(oddList)
new_List = list(map(lambda x: (x * 3), l2))
print(new_List)


def show_even_numbers(limit):
    for j in range(0, limit, 2):
        yield j


eve = list(show_even_numbers(41))
print(eve)
ev = show_even_numbers(41)
print(ev)
print(next(ev))
print(next(ev))
print(next(ev))
print(next(ev))
for p in ev:
    print(p, end=" ")


def table_func(n):
    return lambda m: m * n


print()
create_lambda = table_func(17)
for i in range(1, 11):
    print("17 X {num_iter} = {res_val}".format(num_iter=i, res_val=create_lambda(i)))


# Decorators
def make_highlight(func):
    annotations = ["*", "-", "+", "#", "$", "!"]
    annotation = random.choice(annotations)

    def highlight():
        print(annotation * 50)
        func()
        print(annotation * 50)

    return highlight


def greet_me():
    print("Hi Nitin")


h = make_highlight(greet_me)
h()


@make_highlight
def greet_people(name="Superman"):
    print("Hi", name)


greet_people()


def safe_calculation(func):
    def calculate(*args):
        for r in args:
            if r <= 0:
                raise ValueError("Length of the variable cannot be negative or zero.")
        return func(*args)

    return calculate


@safe_calculation
def area_of_circle(radius):
    return math.pi * radius ** 2


@safe_calculation
def circumference_of_circle(radius):
    return math.pi * radius * 2


@safe_calculation
def area_of_rectangle(length, breadth):
    return length * breadth


print(area_of_circle(5))
# print(area_of_circle(-5))
print(circumference_of_circle(3))
print(area_of_rectangle(4, 5))


# print(area_of_rectangle(-4,5))


def max_list(m: int, l5: list) -> str:
    for k in l5:
        if k > m:
            max_in_list = k
        else:
            max_in_list = m
    return f"Max value is {max_in_list}"
