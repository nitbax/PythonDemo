from setuptools import setup
import setuptools

setup(
    name="PythonDemo",
    version="0.0.1",
    author="Nitin Kumar Baxi",
    description="A small demo on python, pyspark and pandas",
    package_dir={},
    packages=setuptools.find_packages(),
    py_modules=["setup"],
    install_requires=[
        "pyspark==2.4.3",
        "rsa",
        "python-dateutil",
        "pandas",
        "faker",
        "prettytable",
        "cx_oracle",
        "delta",
        "boto3",
    ],
    extras_require={
        "develop": [
            "pyspark-stubs==2.4",
        ]
    },
    entry_points={
        "console_scripts": [
            "startPythonDemo=PythonTutorial.first:main",
            "compare=PandasTutorial.ExcelAndCsvComparison:main",
            "generate=PandasTutorial.RE_Automation:main",
        ]
    },
    python_requires=">=3.7",
)
