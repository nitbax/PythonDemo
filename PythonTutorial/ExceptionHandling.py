a = int(input("Enter one number : "))
b = int(input("Enter 2nd number : "))
try:
    c = a / b
    print(c)
except:
    print("Number is divided by zero.")
else:
    print("Hi, I am in else block now.")
try:
    f = open("C:/Users/nitbax/Desktop/TUTORIALS/Python Tutorial/readFile.txt", "r")
    if f:
        for i in f:
            print(i)
except IOError:
    print("File not found")
else:
    print("File is found")
finally:
    f.close()
try:
    if b == 0:
        raise ArithmeticError
    else:
        print(c)
except:
    print("The value of b can't be 0.")
# Custom Exceptions
