# Intalasi Spark
import os
os.chdir("/")

# !apt-get install openjdk-8-jdk-headless -qq > /dev/null
!wget -q https://archive.apache.org/dist/spark/spark-2.4.1/spark-2.4.1-bin-hadoop2.7.tgz

# 178/179 MB
https://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz


!tar xf spark-2.4.1-bin-hadoop2.7.tgz
!pip install -q findspark
# !pip install -q streamlit

# import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# 09:09 ~/mysite $ echo $JAVA_HOME
# os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-7-openjdk-amd64"

os.environ["SPARK_HOME"] = "/spark-2.4.1-bin-hadoop2.7"

import findspark
findspark.init()
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

import pyspark
from pyspark.context import SparkContext
spark = SparkSession\
        .builder\
        .appName("Map Pada Spark")\
        .getOrCreate()

#sc = SparkContext.getOrCreate()
sc = spark.sparkContext

os.chdir("/content/drive/My Drive/Cisco Big Data Using Python 2021/FGA Big Data Using Python - Kelas B - utk Peserta2/Sesi 1 - 25 Python/flask-sqlite/flask-sqlite-login")
!pwd

print('PySpark Siyaap :D')