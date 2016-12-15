package sinova

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hadoop on 16-12-15.
  */
object SparkSqlHbaseTest {
	def main(args: Array[String]): Unit = {
		val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark sql hbase test")
		val sc = new SparkContext(sparkConf)
		val sqlContext = new SQLContext(sc)

		var hbasetable = sqlContext.read.format("sinova.sparksql.hbase").options(Map(
			"sparksql_table_schema" -> "(key string, title string, url string)",
			"hbase_table_name" -> "t1",
			"hbase_table_schema" -> "(:key , info:name , info:age)"
		)).load()

		hbasetable.printSchema()

		hbasetable.registerTempTable("test")

		var records = sqlContext.sql("SELECT * from test limit 10").collect.foreach(println)
	}
}
