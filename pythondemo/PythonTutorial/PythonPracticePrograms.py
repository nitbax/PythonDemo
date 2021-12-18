# lower = int(input("Enter the lower range: "))
# upper = int(input("Enter the upper range: "))
# Python program to print all Prime numbers between 1 to 50
lower = 1
upper = 50
for n in range(lower, upper + 1):
    if n > 1:
        for i in range(2, n):
            if n % i == 0:
                break
        else:
            print(n, end=" ")

# Python program to find sum of elements in list
total, total1 = 0, 0
list1 = [6, 9, 4, 90, 67]
for element in range(0, len(list1)):
    total = total + list1[element]
print("\nSum of all elements in list1: ", total)
for element in list1:
    total1 = total1 + element
print("Sum of all elements in list2: ", total1)

# Python Program to print duplicates from a list of integer
a = [1, 2, 6, 9, 6, 4, 9, 3, 5, 1]
b = []
print("The Duplicate elements are: ")
for i in range(0, len(a)):
    for j in range(i + 1, len(a)):
        if a[i] == a[j]:
            print(a[j])

print("The Duplicate elements 2 are: ")
for j in a:
    if j not in b:
        b.append(j)
    else:
        print(j, end=" ")

# Python Program to Print all Numbers in a between 1 to 50 Divisible by a Given Number

lower = 1
upper = 50
s = 3
print("\nThe numbers divisible by 3 between 1 to 50 are: ")
for n in range(lower, upper + 1):
    if n % s == 0:
        print(n, end=" ")

# Python Count occurrences of an element in a list

l2 = [2, 3, 6, 3, 5, 3, 1, 1, 2, 3]
e = 3
c = l2.count(e)
print("\nNumber of times the element occurred in list is: ", c)

# Python program to print odd numbers in a List

l3 = [12, 23, 45, 34, 57, 68, 97, 45, 23, 11]
print("The odd numbers are: ")
for n in l3:
    if n % 2 != 0:
        print(n, end=" ")

# Python Program to Reverse a Given Number

n = 1234
r = 0
while n > 0:
    s = n % 10
    r = r * 10 + s
    n = n // 10
print("\nReverse of the given number is: ", r)

# Python Program to Merge Two Lists and Sort it, Union of 2 lists and intersection of 2 lists

l1 = []
l2 = []
s3 = input("Enter the list 1 : ")
s4 = s3.split(",")
print(s4, type(s4))
s4 = list(map(int, s4))
print(s4, type(s4))
s1 = int(input("Enter size of list 1: "))
print("Enter the numbers in list 1: ")
for i in range(s1):
    n1 = int(input("Enter the number: "))
    l1.append(n1)
s2 = int(input("Enter size of list 2: "))
print("Enter the numbers in list 2: ")
for i in range(s2):
    n2 = int(input("Enter the number: "))
    l2.append(n2)
union = list(set().union(l1, l2))
intersection = list(set(l1) & set(l2))
inter = set().intersection(l1, l2)
union.sort()
print("Sorted union list:", union)
intersection.sort()
print("Sorted intersection list:", intersection)
print("Sorted intersection list2:", inter)
