import calendar
import time
from datetime import datetime, date

ticksTillDate = time.time()
print(ticksTillDate)
tupleTime = time.localtime(ticksTillDate)
print(tupleTime)
currentTime = time.asctime(tupleTime)
print(currentTime)
# time.sleep(1)
currDateTime = datetime.now()
print(currDateTime)
d1 = datetime(2020, 2, 22)
print(d1)
d2 = datetime(2020, 2, 22, 2, 8, 10)
print(d2)
d3 = datetime.strptime("2020-03-01", "%Y-%m-%d")
d4 = datetime.date(datetime.strptime("2020-03-01", "%Y-%m-%d"))
print("Date in string : ", d4)
print(d3)
if d1 < d2:
    print("New Date is greater")
elif d1 == d2:
    print("New date is equal to old one")
else:
    print("New Date is less than old one")
start = datetime(2000, 1, 1, 00, 00, 00)
print(start)
start_date = date(2000, 3, 21)
print(date.isoformat(start_date))
print(date.toordinal(start_date))
cal = calendar.month(2020, 2)
print(cal)
calendar.prcal(2020)
