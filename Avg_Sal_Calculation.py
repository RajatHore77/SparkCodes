from pyspark.context import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql import functions as Func
from pyspark.sql.types import *
from pyspark.session import SparkSession
import pandas as pd

sc=SparkContext.getOrCreate()
sqlContext=HiveContext(sc)

sqlContext=SQLContext(sc);
spark=sqlContext.sparkSession

empDt_df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true")
.option("delimiter", ",").load("C:/spark/Datafile/emp_dt_salary.txt")

emp2_df.show()

emp_df.select("Emp_id","emp_name","location").filter(Func.col("location")=="US").show()
emp_df.count()

pandas_df = emp_df.select(emp_df.salary).toPandas()
salList=pandas_df.values.tolist()

def totalAvg(list):
 totalSal=0
 count=0
 for x in list:
   count=count+1
   totalSal=totalSal+x[0]
   
 return totalSal/count 
 
 avgSal=totalAvg(salList)
 newEmpDt_df = empDt_df.withColumn("avgSalary", Func.lit(avgSal))

// another approach using windows function

from pyspark.sql.window import Window
window = Window.partitionBy(empDt_df.address).orderBy(empDt_df.address.desc())
empDt1_df = empDt_df.withColumn("Avg_salary_country_wise",Func.avg(empDt_df.salary).over(window))

/*+------+--------+---------+------+-----------------------+
|emp_id|emp_name|  address|salary|Avg_salary_country_wise|
+------+--------+---------+------+-----------------------+
|     4|   Tanya|   Russia|  7500|                23750.0|
|     7|   Jerry|   Russia| 40000|                23750.0|
|     1|     Tim|       US|  4800|                 5750.0|
|     2|  George|       US|  3200|                 5750.0|
|     8|   Cathy|       US|  5000|                 5750.0|
|    10|   Peter|       US| 10000|                 5750.0|
|     3|    Mary|       UK|  8000|                 6700.0|
|     6|     Jim|       UK|  5400|                 6700.0|
|     5|    Rose|Australia|  7000|                13500.0|
|     9|    Andy|Australia| 20000|                13500.0|
+------+--------+---------+------+-----------------------+ */
