- 👋 Hi, I’m @nitbax
- 👀 I’m interested in Data Engineering and Data Science
- 🌱 I’m currently learning Machine learning
- 💞️ I’m looking to collaborate on ...
- 📫 How to reach me ...

# To build the PythonDemo trainings

Install Python  version> 3.7 and then run the below commands
- pip install wheel
- pip install egg
- pip install setuptools --upgrade
- pip install .[develop]
- Now you can run any programs

# To build the GlueDemo trainings

- 👋 Install Ubuntu from Microsoft Store
- Run below commands to complete the installations Required in Ubuntu
- sudo apt update
- sudo apt install software-properties-common
- sudo add-apt-repository ppa:deadsnakes/ppa
- sudo apt update
- sudo apt install python3.7
- sudo apt update
- sudo apt install openjdk-8-jdk openjdk-8-jre
- sudo apt install openjdk-8-jre
- sudo apt install zip
- sudo apt install python-is-python3 (For Ubuntu 20.04)

🌱 Verify Java and Python Installations
- java -version
- python3 --version

🌱 Follow the steps mentioned in the aws docs link (https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html)
- git clone https://github.com/awslabs/aws-glue-libs.git

💞️ Environment Variables Setting (Check if Maven and Spark is downloaded in local, if not then first download in local)
- export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
- export JRE_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
- export PATH=$PATH:$JAVA_HOME/bin
- export M2_HOME=/mnt/c/apache-maven-3.8.1
- export PATH=$PATH:$M2_HOME/bin
- export SPARK_HOME=/home/nitbax/spark_2
- export PATH=$PATH:$SPARK_HOME/bin
- alias python=python3
- alias pip=pip3
- sudo ln -s /usr/bin/python3 /usr/bin/python (Symlink python to python3 in ubuntu 18.04)
- sudo ln -sf /usr/bin/python3.7 /usr/bin/python3 (Symlink Python3 to any python version, can be used For Glue 2.0)

📫 After Python is installed, run the below commands
- pip install wheel
- pip install black
- pip install pytest

Copy the jar files from local to jars folder in SPARK_HOME/jars folder
- cp mnt/c/Users/nitba/.ivy2/jars/*.jar .
For Glue Pyspark shell
- ./bin/gluepyspark

Running Glue jobs(Python) from local
- ./bin/gluesparksubmit --master local[*] --name test_job /home/nitbax/glue_demo/ExcelToParquetDemo.py

Windows Set up files for Testing
- pyspark --master local[*] --packages com.crealytics:spark-excel_2.11:0.11.1
- spark.read.format("com.crealytics.spark.excel").option("useHeader", "true").load("C:/Users/nitba/Data Samples/Sample1.xlsx").show()
- spark-submit --master local[*] --name test_job /home/nitbax/glue_demo/ExcelToParquetDemo.py
<!---
nitbax/nitbax is a ✨ special ✨ repository because its `README.md` (this file) appears on your GitHub profile.
You can click the Preview link to take a look at your changes.
--->
