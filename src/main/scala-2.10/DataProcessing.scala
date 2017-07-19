import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object DataProcessing {

  val conf = new SparkConf().setAppName("SparkSolution").setMaster("local")
  val sparkContext = new SparkContext(conf)
  val sparkSession = SparkSession.builder().config(conf).getOrCreate()

  def readDatasetFromXML(rowTag: String, fileName: String): DataFrame = {
    val filePath = "./src/main/resources/" + fileName
    val xmlFile = sparkSession.read
          .format("com.databricks.spark.xml")
          .option("rowTag", rowTag)
          .load(filePath)

    val xmlSchemaFixed = xmlFile.selectExpr("explode(row) as row").select("row.*")

    return xmlSchemaFixed
  }

  def countGroupedValues(datasetGrouped: RelationalGroupedDataset) : DataFrame ={
    return datasetGrouped.count()
  }

  def selectColumnsWithNames(dataFrame:DataFrame, columnNames: String*) : DataFrame ={
    return dataFrame.select(columnNames.head, columnNames.tail:_*)
  }

  def orderDescendingWithColumn(dataFrame: DataFrame, columnToOrderBy: String): DataFrame = {
    return dataFrame.orderBy(dataFrame.col(columnToOrderBy).desc)
  }

  def filterNullValues(dataFrame: DataFrame): DataFrame = {
    return dataFrame.filter(row => !row.anyNull)
  }

  def mapDataSetToStringArray(mapFunction: Row => Traversable[String], dataFrame: DataFrame): DataFrame ={
    import sparkSession.implicits._
    val mappedDataFrame = dataFrame.flatMap(mapFunction).withColumnRenamed("value","tags")

    return mappedDataFrame.toDF()
  }

  def groupDataFrameByColumn(dataFrame: DataFrame, columName: String): RelationalGroupedDataset = {
   return dataFrame.groupBy(columName)
  }
}
