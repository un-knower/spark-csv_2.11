package test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by ideal on 17-3-23.
  * //"\u0001" 正式分割符号
  */
object TestLoadCsv {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder.
      master("local[2]")    //.setMaster("local[*]") yarn-cluster
      .appName("index")
      .config("spark.some.config.option", "some-value")
      //.enableHiveSupport()  //支持hive
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val options = Map("header" -> "true",
      "path" -> "/home/ideal/desktop/data/data/1.txt"
     ,"delimiter"->"#","comment"->null)

    val customSchema = StructType(Array(
      StructField("user_id", IntegerType, true),
      StructField("city", StringType, true),
      StructField("cellid", StringType, true),
      StructField("kpi1", StringType, true),
      StructField("blank", StringType, true)))
//
    val newStudents = spark.read.options(options).schema(customSchema)
      .format("com.broadtech.spark.csv").load()

    newStudents.show(5)
    newStudents.printSchema()
  }

  /**
    * 把rdd 的column补全
    * */

  def rddColumn(spark: SparkSession,rdd1:RDD[String],columns:String,split:String): Unit ={

    val rd1=rdd1.map(_.split(split))

  }

  /**
    *把rdd1和rdd2 横向合并 并去掉重复字段
    * */
  def rddJoin(rdd1:RDD[String], rdd2:RDD[String],split:String): RDD[String] ={
    rdd1.zip(rdd2).map(x=>x._1+split+x._2)
  }

}
