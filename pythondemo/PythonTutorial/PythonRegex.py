import re

s1 = "Hi Nitin,How are you? How is everything?"
mRes = re.match("How", s1)
print(mRes)
allMatches = re.findall("How", s1)
print(allMatches)
sRes = re.search("How", s1)
print(sRes)
sRes1 = re.search("how", s1, re.RegexFlag.IGNORECASE)
print(sRes1)
print(sRes1.span())
print(sRes1.group())
print(sRes1.string)
