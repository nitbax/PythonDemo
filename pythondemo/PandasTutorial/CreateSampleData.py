import random
from datetime import datetime, timedelta, date
import pandas as pd
from faker import Faker


def generate_dummy_data(num_of_records, date_format="withTime"):
    fake = (
        Faker()
    )  # Default locale is 'en_US', 'hi_IN' and 'en_GB' are some other locales
    dummy_data = [
        {
            "STAFF_ID": random.randint(123456789, 999999999),
            "CREATED_DATE": gen_datetime(2012, date_format=date_format),
            "NAME": fake.name(),
            "EMAIL": fake.email(),
            "Phone Number": fake.phone_number(),
            # "Address": fake.address(),
            "Zip Code": fake.zipcode(),
            "City": fake.city(),
            "State": fake.state(),
            "Country": fake.country(),
            "DummyString": fake.pystr(min_chars=None, max_chars=10),
            "text": fake.word(),
            "DummyBoolean": fake.pybool(),
            "PGM_ID": random.uniform(123456789, 999999999),
        }
        for i in range(num_of_records)
    ]
    return dummy_data


def generate_pgm_data(num_of_records, date_format="withTime"):
    fake = (
        Faker()
    )  # Default locale is 'en_US', 'hi_IN' and 'en_GB' are some other locales
    dummy_data = [
        {
            "PGM_CODE": fake.pystr(min_chars=3, max_chars=3),
            "ID": random.randint(123456789, 999999999),
            "CASE_ID": random.randint(123456789, 999999999),
            "CREATED_BY": fake.name(),
            "UPDATED_BY": fake.name(),
            "CREATED_ON": gen_datetime(2012, date_format=date_format),
            "UPDATED_ON": gen_datetime(2012, date_format=date_format),
            "MFG_SIGN_DATE": gen_datetime(2012, date_format=date_format),
            "INTAKE_REDETER_IND": random.choice(["Y", "N"]),
            "PMT_PREF_CODE": fake.pystr(min_chars=3, max_chars=3),
        }
        for i in range(num_of_records)
    ]
    return dummy_data


def gen_datetime(min_year=2000, date_format="withTime", max_year=datetime.now().year):
    if date_format == "withTime":
        # generate a datetime in format yyyy-mm-dd hh:mm:ss.000000
        start = datetime(min_year, 1, 1, 00, 00, 00)
    else:
        # generate a datetime in format yyyy-mm-dd
        start = date(min_year, 1, 1)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()
    # generates a datetime in format yyyy-mm-dd hh:mm:ss
    # return start + (end - start) * random.randint(1,10)


if __name__ == "__main__":
    records = 20
    # Date and Timestamp generation check
    random_date = gen_datetime(min_year=2013, date_format="onlyDate")
    print(random_date)
    start1 = datetime(2012, 1, 1, 00, 00, 00)
    print("Start date :", start1)
    years1 = 2020 - 2012 + 1
    end1 = start1 + timedelta(days=365 * years1)
    print("End Date :", end1)
    td = start1 + (end1 - start1) * random.randint(1, 10)
    print("Time Difference:", td)
    random_date1 = gen_datetime(min_year=2013, max_year=2020)
    print(random_date1)
    # Create DataFrames which will be used to create CSV files
    df = pd.DataFrame(generate_dummy_data(records))
    df1 = pd.DataFrame(generate_dummy_data(records, "withoutTime"))
    df.to_csv("C:/Test Data/testData.csv", index=False)
    df1.to_csv("C:/Test Data/testData1.csv", index=False)
    pgm = pd.DataFrame(generate_pgm_data(records))
    pgm.to_csv("C:/Test Data/Report/pgm.csv", index=False)
    print("Dummy Data creation complete!")
