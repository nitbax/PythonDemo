# Python Conditional and Loop Statements
a = 10
if a % 2 == 0:
    print("Even Number")
else:
    print("Odd Number")

b = 20
if b > 0:
    print("Positive number.")
elif b < 0:
    print("Negative number.")
else:
    print("Number is zero.")

# Loops
i = 1
n = 11
for i in range(1, n):
    print("Values in loop = {0}".format(i))
for i in range(1, n):
    print(i, end=" ")
print("\n")
print("Print the table of : {0}".format(n))
for i in range(1, n):
    res = n * i
    # 2 ways to print integer and string on the same line.
    print(str(n) + " X " + str(i) + " = " + str(res))
    print("{0} X {1} = {2}".format(n, i, n * i))
a, b = 0, 0
c = 9
# number of spaces
k = 2 * c - 2
for a in range(0, c):
    for b in range(0, k):
        print(end=" ")
    k = k - 1
    m = a + 1
    """
    for b in range(0,a+1):
            print("* ",end="")"""
    print("* " * m)
r = 5
j = 0
for j in range(0, r):
    print(j)
else:
    print("Out of loop and there is no break statement.")
for j in range(0, r):
    print(j)
    break
else:
    print("Out of loop if there is no break statement.")
l = 1
maxi = int(input("Enter the max input: "))
print("Print the table of {0}".format(maxi))
while l <= 10:
    print("{0} X {1} = {2}".format(maxi, l, maxi * l))
    l = l + 1
else:
    print("No break so printing else in while loop.")
n = 1
checkMax = 5
print("Print the table of {0}".format(checkMax))
while n <= 10:
    print("{0} X {1} = {2}".format(checkMax, n, checkMax * n))
    l = l + 1
    break
else:
    print("If there is no break then print this.")
