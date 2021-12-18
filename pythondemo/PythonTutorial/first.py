def main():
    """These
            Are
    Some Python starter examples"""
    print("Hello World !")
    # Tuples example
    tuple1 = ("Nitin", 135, "B.Tech College")
    print(tuple1)
    print(tuple1[0])
    tuple2 = ("Neelabh", 127, "B.Tech College")
    print(tuple2)
    print(tuple1 + tuple2)
    dictionary = {"name": "Nitin", "roll": 135, "college": "B.Tech"}
    print(dictionary)
    print(dictionary.keys())
    print(dictionary.values())
    print(dictionary["name"])
    print(type(dictionary))
    str1 = "Hello Nitin"
    str2 = "I know you are Superman"
    print(str1)
    print(str2)
    print(str1[0:5])
    print(str1[6])
    print(str1 * 2)
    print(str1 + ", " + str2)
    l = [1, "Hi", "Kumar", "Baxi"]
    print(l)
    print(l[2:])
    print(l[2])
    print(l[0:3])
    print(l + l)
    print(l * 3)
    l[3] = "Velli"
    print(l)
    print(1 in l)
    t = (2, "Hi", "Item", "Kumari")
    print(t)
    print(t[2:])
    print(t[2])
    print(t[1:3])
    print(t + t)
    print(t * 3)
    print(1 in t)
    d = {1: "Atul", 2: "Nitin", 3: "Ujjwal", 4: "Sandeep"}
    print(d)
    print("The name of AM is : " + d[1])
    print("The name of TL is : " + d[2])
    print(d.keys())
    print(d.values())
    a = 42
    print(a)
    a = 20
    print(a)
    b = 10
    print(a + b)
    print(a / b)
    print(a % b)
    print(a // b)
    print("The frequency of india is {freq}".format(freq=50))
    print("The frequency of USA is %s" % (60))
    s1 = ""
    s1 = "Nitin"
    if s1 != "":
        print("Khyati")
    else:
        print("Nitin")
    for i in range(0, 2):
        print("Khyati")


if __name__ == "__main__":
    main()
