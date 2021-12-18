import pyspark.sql.functions as fx
from pyspark.sql import SparkSession, Window

spark = SparkSession.builder.appName("practice").master("local").getOrCreate()
spark.conf.set("spark.sql.caseSensitive", "False")
practice_path = "C:/Users/nitbax/TUTORIALS/SparkTutorial/Spark Practice/"

emp = (
    spark.read.option("header", "True")
    .csv(practice_path + "employees.csv")
    .alias("emp")
)
dept = (
    spark.read.option("header", "True")
    .csv(practice_path + "department.csv")
    .alias("dept")
)


def main():
    practice1()
    practice2()
    practice3()
    practice4()
    practice5()
    practice6()
    practice7()
    practice8()


def practice1():
    """Manager Details from each Department."""
    emp.join(dept.where(fx.col("Level") == "Manager").hint("broadcast"), ["Id"]).show(
        truncate=False
    )


def practice2():
    """Total Salary of employees per dept."""
    # Way 1
    emp.join(dept, ["Id"]).groupby(fx.col("Department")).agg(
        fx.sum(fx.col("Salary"))
    ).show(truncate=False)
    # Way 2
    emp.join(dept, ["Id"]).groupby(fx.col("Department")).agg({"Salary": "sum"}).show(
        truncate=False
    )


def practice3():
    """Employee Details and count belongs to AM and joined post 2010."""
    emp_details = emp.where(
        fx.year(fx.to_date(fx.col("Joining Date"), "yyyy/MM/dd")) > 2010
    ).join(dept.where(fx.col("Level") == "AM"), ["Id"])
    emp_details.show(truncate=False)
    print(emp_details.count())


def practice4():
    """Highest and lowest Salaried employee per department with Department name ."""
    emp.join(dept, ["Id"]).groupby(fx.col("Department")).agg(
        fx.max(fx.col("Salary")), fx.min(fx.col("Salary"))
    ).show(truncate=False)


def practice5():
    """Employee details having salary greater the 18,000 from each department."""
    emp.where(fx.col("Salary") > 18000).join(dept, ["Id"]).orderBy(
        "Department", "Salary", ascending=True
    ).show(truncate=False)


def practice6():
    """Each row from employee table should be appended with total salary of employee
    table (using window function)."""
    window_spec = Window.rangeBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )
    emp.withColumn("Total Salary", fx.sum(fx.col("Salary")).over(window_spec)).show(
        truncate=False
    )


def practice7():
    """Employee details for which department info is missing in dept table and Department_name
    which has no employee."""
    emp.join(dept.hint("broadcast"), ["Id"], "outer").where(
        (fx.col("Department").isNull()) | (fx.col("Name").isNull())
    ).show(truncate=False)


def practice8():
    """Total no of employee per Joining date(consider only year)."""
    emp.join(dept, ["Id"]).groupby(
        fx.year(fx.to_date(fx.col("Joining Date"), "yyyy/MM/dd"))
    ).agg(fx.count(fx.col("Id"))).show(truncate=False)


if __name__ == "__main__":
    main()
