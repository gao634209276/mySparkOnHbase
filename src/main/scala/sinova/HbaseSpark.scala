package sinova

import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Hello world!
  */
object HbaseSpark {
	def main(args: Array[String]): Unit = {

		// 本地模式运行,便于测试
		val sparkConf = new SparkConf().setMaster("local").setAppName("HBaseTest")

		// 创建hbase configuration
		val hBaseConf = HBaseConfiguration.create()
		hBaseConf.set(TableInputFormat.INPUT_TABLE, "hivehbase")

		// 创建 spark context
		val sc = new SparkContext(sparkConf)
		val sqlContext = new SQLContext(sc)
		import sqlContext.implicits._

		// 从数据源获取数据
		val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

		// 将数据映射为表  也就是将 RDD转化为 dataframe schema
		val shop = hbaseRDD.map(r => (
				//Bytes.toString(CellUtil.cloneRow(r._2.getRow())),
				Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("customer_id"))),
				Bytes.toString(r._2.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")))
				)).toDF("id", "name")

		shop.registerTempTable("shop")

		// 测试
		val df2 = sqlContext.sql("SELECT customer_id FROM shop")

		df2.foreach(println)
	}
}
