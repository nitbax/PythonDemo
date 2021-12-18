# Collections

# lists
l = [1, 2, 3, "Nitin", "Kumar", "Baxi"]
print("Print the list : ", l)
print("Print the list : {}".format(l))
for i in l:
    print(i)
print("Print numbers in a list : {}".format(l[0:3]))
print("Print strings in a list : {}".format(l[3:6]))
print("Print the contains conditon : {}".format(2 in l))
l1 = [4, 5, 6, "Namit", "Gaur"]
print("Print list concatenation {}".format(l + l1))
l.append("TL")
l.append(9)
print("Add elements in the list {}".format(l))
l.remove("TL")
l.remove(9)
print("Removing elements in the list {}".format(l))
print("Print type of l:", type(l))
l.extend(["is", "Superman", 1])
print("Added multiple elements at once", l)
l.insert(3, 4)
print(l)
l[9] = "Kal-El"
print("Updated List", l)
l1 = l.copy()
print("Deep copy of list:", l1)  # Any operations on l will not effect l1
l2 = l
print("Shallow copy of List:", l2)  # Any operations on l will effect l2 as well

# tuples
t = (11, 12, 13, "Nitin", "Kumar", "Baxi")
print("Print the tuple {}".format(t))
for k in t:
    print(k)
print(t[:])
print(t[0:])
print("Print numbers in the tuple : {}".format(t[:3]))
print("Print strings in the tuple : {}".format(t[3:]))
print("Print last element in the tuple : {}".format(t[-1]))
Employees = [
    (101, "Nitin", 22),
    (102, "Anil", 29),
    (103, "Pankaj", 45),
    (104, "Navnish", 34),
]
print("----Printing list----")
for i in Employees:
    print(i)
Employees[0] = (110, "Kumar", 22)
print()
print("----Printing list after modification----")
for i in Employees:
    print(i)

# Sets
s = {"Nitin", "is", "Superman", 1, "Set"}
print(s)
s1 = set(["Creating", "Set", "with", "set() method"])
print(s1)
for i in s1:
    print(i)
months = {"Jan", "Feb", "Mar", "Apr", "May", "June", "Jan"}
print("Months in a year : {}".format(months))
months.add("July")
months.add("August")
print("Modified Months in a year : {}".format(months))
for i in months:
    print(i)
months.update(["Sep", "Oct", "Nov", "Dec"])
print("Updated Months in a year : {}".format(months))
s.remove(1)
print("Updated first set : {}".format(s))
s.discard(1)
print("Union of sets : {}".format(s | s1))
print("Union of sets using union method : {}".format(s.union(s1)))
print("Intersection of sets : {}".format(s & s1))
print("Intersection of sets using intersection method : {}".format(s.intersection(s1)))
a = {"ayush", "bob", "castle"}
b = {"castle", "dude", "emyway"}
c = {"fuson", "gaurav", "castle"}
a.intersection_update(b, c)
print(a)
print(b - c)
print(b.difference(c))
d = {"castle"}
print("Set comparisons : ")
print(a == b)
print(a == c)
print(a == d)
print(a > b)
print(a > c)
print(a < b)
print(a < c)
frozenSet = frozenset([1, 2, 3, 4, 5])
print("Frozen set : {}".format(frozenSet))

# Dictionary
d = {1: "Nitin", 2: "is", 3: "Clark", 4: "Kent"}
print(d)
frozenSetKeys = frozenset(d)
print(frozenSetKeys)
print("First element of dictionary : {}".format(d[1]))
d[1] = "Kumar"
d[2] = "becomes"
d[3] = "Super"
d[4] = "Man"
print("Updated dictionary values : {}".format(d))
del d[4]
print("Modified dictionary values : {}".format(d))
emp = {"Name": "Nitin", "Id": "1", "Post": "TL", "Id": "is"}
print(emp)
for x in emp:
    print(x)
for x in emp:
    print(emp[x])
for x in emp.values():
    print(x)
for x in emp.items():
    print(x)
for x, y in emp.items():
    print(x, y)
    print(x)
    print(y)

d.update(emp)
print("Add 1 dictionary with another : {}".format(d))
