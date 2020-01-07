import kafka.serializer.StringDecoder
import io.confluent.kafka.serializers.KafkaJsonDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils


object RetailConsumer{
  

  
  def main(args : Array[String]) {
    
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> "********",
	  "schema.registry.url" -> "*****************",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "io.confluent.kafka.serializers.KafkaJsonDeserializer",
      "group.id" -> "example",
      "auto.offset.reset" -> "smallest",
      "enable.auto.commit" -> "false")
   
	
   
		
		val conf = new SparkConf().setAppName("SimpleStreamingApp").setMaster("local[2]")
		conf.set("spark.testing.memory", "471859200")
		//val sc = SparkContext.getOrCreate(conf)
		val ssc = new StreamingContext(conf, Seconds(60))
		val sqlContext = org.apache.spark.sql.SQLContext.getOrCreate(SparkContext.getOrCreate(conf))		
		val topicsSet = "retail-data".split(",").toSet
		   
		val directKafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc , kafkaParams ,topicsSet)
		   
		/*directKafkaStream.foreachRDD { rdd =>
			  //val streams = KafkaUtils.createDirectStream(ssc , kafkaParams ,topicsSet)
		   // sqlContext.read.json(directKafkaStream.map(x => x._2)).show()
			 println("*** got an RDD, size = " + rdd.count())
			  rdd.foreach{s => 
				println(s._2)
			    val df = sqlContext.read.json(s._2)
				df.show
				
			 }
			}*/
		
		/*directKafkaStream.foreachRDD{rdd =>
		  rdd.foreach{ s=>
		    println(s._2)
		    
		  }
		  val validJsonRdd = rdd.map(x => x._2)
		  
		    val df = sqlContext.read.json(validJsonRdd)
		    //val df = s._2.
				df.show
    }*/
    	directKafkaStream.foreachRDD{ rdd => 
                 //println ("records value="+rdd.map(_._2))
                 val dataFrame = sqlContext.read.json(rdd.map(_._2))
                 dataFrame.show() 
		        val deliveryDF=dataFrame.select("order_id", "order_date" , "order_customer_id" ,"order_status")
				deliveryDF.show()
		 //val deliveryStatusDF=dataFrame.withColumn("delivery_status", when(dataFrame("order_item_order_id") === 1, "In progress").when(dataFrame("order_item_order_id") === 2, "Shipped").otherwise("complete")).show()
		 //val countValue=dataFrame.count()
		 //println ("records count value="(dataFrame.count().toInt))
		 deliveryDF.groupBy("order_status").count().show()
		 val orderStatusDF=deliveryDF.select(col("order_id"),col("order_date"), col("order_customer_id"), col("order_status"),row_number().over(Window.partitionBy(col("order_id")).orderBy(col("order_date") desc)).alias("Order_Status_Sequence"))
                 orderStatusDF.show()                  
                 val latestOredrStatusDF=orderStatusDF.filter("Order_Status_Sequence==1")
                 latestOredrStatusDF.show()		 
    	}
		
		ssc.start()	      // Start the computation
		ssc.awaitTermination()  // Wait for the computation to terminate
			//println(sc.isStopped)
	
	       
  }
  
}
