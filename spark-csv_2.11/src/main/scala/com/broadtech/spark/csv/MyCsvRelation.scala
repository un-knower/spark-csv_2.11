package com.broadtech.spark.csv

import com.broadtech.spark.csv.util.TextFile
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, InsertableRelation, PrunedScan, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by ideal on 17-3-30.
  * 这里是spark 数据加载器
  */
case class MyCsvRelation protected[spark](
                                            charset: String,
                                            path: String,
                                            useHeader: Boolean,
                                            delimiter: Char,
                                            quote: Character,
                                            escape: Character,
                                            comment: Character,
                                            parseMode: String,
                                            parserLib: String,
                                            ignoreLeadingWhiteSpace: Boolean,
                                            ignoreTrailingWhiteSpace: Boolean,
                                            treatEmptyValuesAsNulls: Boolean,
                                            userSchema: StructType = null,
                                            inferCsvSchema: Boolean,
                                            codec: String = null,
                                            nullValue: String = "",
                                            dateFormat: String = null,
                                            maxCharsPerCol: Int = 100000
)(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with PrunedScan with InsertableRelation {

  private var myrdd:RDD[Row]=null

  /**
    * 遍历文件
    * */
  def loadrun(): Unit ={
    val conf= sqlContext.sparkContext.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(conf)
    val files=hdfs.listStatus(new Path(path)).map(_.getPath.toString)
    files.map(file=>
        CsvRelation(
          () => TextFile.withCharset(sqlContext.sparkContext, file, charset),
          Some(file),
          useHeader,
          delimiter,
          quote,
          escape,
          comment,
          parseMode,
          parserLib,
          ignoreLeadingWhiteSpace,
          ignoreTrailingWhiteSpace,
          treatEmptyValuesAsNulls,
          userSchema,
          inferCsvSchema,
          codec,
          nullValue,
          dateFormat,
          maxCharsPerCol)(sqlContext)
    ).foreach(csvRelation=>{
      //sqlContext.baseRelationToDataFrame(csvRelation).show()
      //sqlContext.createDataFrame(csvRelation.buildScan,userSchema).show()
      addRdd(csvRelation.buildScan)
    })

  }


  override def buildScan(): RDD[Row] = {loadrun();myrdd}
  override def buildScan(requiredColumns: Array[String]): RDD[Row] = buildScan()
  override def insert(data: DataFrame, overwrite: Boolean): Unit = ???


  def addRdd(rdd:RDD[Row]): Unit ={
    if(myrdd==null)
      myrdd=rdd
    else
      myrdd=myrdd.++(rdd)
  }

  override def schema: StructType = userSchema
}
