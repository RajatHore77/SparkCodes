import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object sparkScalaJoinExample. {
  val conf = new SparkConf().setMaster("local[*]");
  val sc = SparkSession.builder().config(conf).appName("Spark").getOrCreate().sparkContext;
  
  def main(args:Array[String]){
  
     val sqlContext = new SQLContext(sc)
     val jsonData_1 = sqlContext.read.json("/employees_singleLine.json")

     val jsonData_2 = sqlContext.read.json(sc.wholeTextFiles("/employees_multiLine.json").values)

     val leftJoinDF=jsonData_1.join(jsonData_2, jsonData_1("empno") === jsonData_2("empno"), "left").filter(jsonData_2("empno")isNotNull).select(jsonData_1("ename"),jsonData_1("sal"))
     leftJoinDF.show()

     val rightJoinDF=jsonData_1.join(jsonData_2, jsonData_1("empno") === jsonData_2("empno"), "right").filter(jsonData_1("empno")isNotNull).select(jsonData_2("ename"),jsonData_2("sal"))
     rightJoinDF.show()   
  
   }
 }
