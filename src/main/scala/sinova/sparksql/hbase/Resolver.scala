package sinova.sparksql.hbase

import java.io.Serializable
import org.apache.spark.sql.sources.TableScan
import org.apache.hadoop.hbase.client.{Result}
import org.apache.spark.sql._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.sources.BaseRelation
import sinova.sparksql.hbase.hbase._


object Resolver extends Serializable {
	def resolve(hbaseField: HBaseSchemaField, result: Result): Any = {
		val cfColArray = hbaseField.fieldName.split(":", -1)
		val cfName = cfColArray(0)
		val colName = cfColArray(1)
		var fieldRs: Any = null
		//resolve row key otherwise resolve column
		if (cfName == "" && colName == "key") {
			fieldRs = resolveRowKey(result, hbaseField.fieldType)
		} else {
			fieldRs = resolveColumn(result, cfName, colName, hbaseField.fieldType)
		}
		fieldRs
	}

	def resolveRowKey(result: Result, resultType: String): Any = {
		val rowkey = resultType match {
			case "string" =>
				result.getRow.map(_.toChar).mkString
			case "int" =>
				result.getRow.map(_.toChar).mkString.toInt
			case "long" =>
				result.getRow.map(_.toChar).mkString.toLong
		}
		rowkey
	}

	def resolveColumn(result: Result, columnFamily: String, columnName: String, resultType: String): Any = {
		val column = result.containsColumn(columnFamily.getBytes, columnName.getBytes) match {
			case true => {
				resultType match {
					case "string" =>
						Bytes.toString(result.getValue(columnFamily.getBytes, columnName.getBytes))
					//result.getValue(columnFamily.getBytes,columnName.getBytes).map(_.toChar).mkString
					case "int" =>
						Bytes.toInt(result.getValue(columnFamily.getBytes, columnName.getBytes))
					case "long" =>
						Bytes.toLong(result.getValue(columnFamily.getBytes, columnName.getBytes))
					case "float" =>
						Bytes.toFloat(result.getValue(columnFamily.getBytes, columnName.getBytes))
					case "double" =>
						Bytes.toDouble(result.getValue(columnFamily.getBytes, columnName.getBytes))
				}
			}
			case _ => {
				resultType match {
					case "string" =>
						""
					case "int" =>
						0
					case "long" =>
						0
				}
			}
		}
		column
	}
}

/**
  * val hbaseDDL = s"""
  * |CREATE TEMPORARY TABLE hbase_people
  * |USING com.shengli.spark.hbase
  * |OPTIONS (
  * |  sparksql_table_schema   '(row_key string, name string, age int, job string)',
  * |   hbase_table_name     'people',
  * | hbase_table_schema '(:key , profile:name , profile:age , career:job )'
  * |)""".stripMargin
  */
case class HBaseRelation(@transient val hbaseProps: Map[String, String])(@transient val sqlContext: SQLContext) extends BaseRelation with Serializable with TableScan {

	val hbaseTableName = hbaseProps.getOrElse("hbase_table_name", sys.error("not valid schema"))
	val hbaseTableSchema = hbaseProps.getOrElse("hbase_table_schema", sys.error("not valid schema"))
	val registerTableSchema = hbaseProps.getOrElse("sparksql_table_schema", sys.error("not valid schema"))
	val rowRange = hbaseProps.getOrElse("row_range", "->")
	//get star row and end row
	val range = rowRange.split("->", -1)
	val startRowKey = range(0).trim
	val endRowKey = range(1).trim

	val tempHBaseFields = extractHBaseSchema(hbaseTableSchema)
	//do not use this, a temp field
	val registerTableFields = extractRegisterSchema(registerTableSchema)
	val tempFieldRelation = tableSchemaFieldMapping(tempHBaseFields, registerTableFields)

	val hbaseTableFields = feedTypes(tempFieldRelation)
	val fieldsRelations = tableSchemaFieldMapping(hbaseTableFields, registerTableFields)
	val queryColumns = getQueryTargetCloumns(hbaseTableFields)

	def feedTypes(mapping: Map[HBaseSchemaField, RegisteredSchemaField]): Array[HBaseSchemaField] = {
		val hbaseFields = mapping.map {
			case (k, v) =>
				val field = k.copy(fieldType = v.fieldType)
				field
		}
		hbaseFields.toArray
	}

	def isRowKey(field: HBaseSchemaField): Boolean = {
		val cfColArray = field.fieldName.split(":", -1)
		val cfName = cfColArray(0)
		val colName = cfColArray(1)
		if (cfName == "" && colName == "key") true else false
	}

	//eg: f1:col1  f1:col2  f1:col3  f2:col1
	def getQueryTargetCloumns(hbaseTableFields: Array[HBaseSchemaField]): String = {
		var str = ArrayBuffer[String]()
		hbaseTableFields.foreach { field =>
			if (!isRowKey(field)) {
				str += field.fieldName
			}
		}
		str.mkString(" ")
	}

	lazy val schema = {
		val fields = hbaseTableFields.map { field =>
			val name = fieldsRelations.getOrElse(field, sys.error("table schema is not match the definition.")).fieldName
			val relatedType = field.fieldType match {
				case "string" =>
					SchemaType(StringType, nullable = false)
				case "int" =>
					SchemaType(IntegerType, nullable = false)
				case "long" =>
					SchemaType(LongType, nullable = false)
			}
			StructField(name, relatedType.dataType, relatedType.nullable)
		}
		StructType(fields)
	}

	def tableSchemaFieldMapping(externalHBaseTable: Array[HBaseSchemaField], registerTable: Array[RegisteredSchemaField]): Map[HBaseSchemaField, RegisteredSchemaField] = {
		if (externalHBaseTable.length != registerTable.length) sys.error("columns size not match in definition!")
		val rs = externalHBaseTable.zip(registerTable)
		rs.toMap
	}

	/**
	  * spark sql schema will be register
	  * registerTableSchema   '(rowkey string, value string, column_a string)'
	  */
	def extractRegisterSchema(registerTableSchema: String): Array[RegisteredSchemaField] = {
		val fieldsStr = registerTableSchema.trim.drop(1).dropRight(1)
		val fieldsArray = fieldsStr.split(",").map(_.trim)
		fieldsArray.map { fildString =>
			val splitedField = fildString.split("\\s+", -1)
			RegisteredSchemaField(splitedField(0), splitedField(1))
		}
	}

	//externalTableSchema '(:key , f1:col1 )'
	def extractHBaseSchema(externalTableSchema: String): Array[HBaseSchemaField] = {
		val fieldsStr = externalTableSchema.trim.drop(1).dropRight(1)
		val fieldsArray = fieldsStr.split(",").map(_.trim)
		fieldsArray.map(fildString => HBaseSchemaField(fildString, ""))
	}


	// By making this a lazy val we keep the RDD around, amortizing the cost of locating splits.
	lazy val buildScan = {

		val hbaseConf = HBaseConfiguration.create()
		hbaseConf.set("hbase.zookeeper.quorum", "hadoop")
		hbaseConf.set(TableInputFormat.INPUT_TABLE, hbaseTableName)
		hbaseConf.set(TableInputFormat.SCAN_COLUMNS, queryColumns);
		hbaseConf.set(TableInputFormat.SCAN_ROW_START, startRowKey);
		hbaseConf.set(TableInputFormat.SCAN_ROW_STOP, endRowKey);

		val hbaseRdd = sqlContext.sparkContext.newAPIHadoopRDD(
			hbaseConf,
			classOf[org.apache.hadoop.hbase.mapreduce.TableInputFormat],
			classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
			classOf[org.apache.hadoop.hbase.client.Result]
		)


		val rs = hbaseRdd.map(tuple => tuple._2).map(result => {
			var values = new ArrayBuffer[Any]()
			hbaseTableFields.foreach { field =>
				values += Resolver.resolve(field, result)
			}
			Row.fromSeq(values.toSeq)
		})
		rs
	}

	private case class SchemaType(dataType: DataType, nullable: Boolean)

}