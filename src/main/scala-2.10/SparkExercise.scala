import org.apache.spark.sql.{DataFrame, Row}


class SparkExercise {

  def findLocationOfUserWithMaxUpVotes(fileContentParsed: DataFrame): String = {


    val upVotesAndLocation = DataProcessing.selectColumnsWithNames(fileContentParsed, "_Location", "_upVotes","_displayName")
    val orderedUpVotesAndLocation = DataProcessing.orderDescendingWithColumn(upVotesAndLocation, "_UpVotes")
    val upVotesTop = orderedUpVotesAndLocation.first()
    return s"User with highest number of upvotes is: ${upVotesTop.getAs("_displayName")} " +
      s"is located: ${upVotesTop.getAs("_Location")} and has ${upVotesTop.getAs("_upVotes")} up votes!"
  }

  def findLocationOfUserWithMaxDownVotes(fileContentParsed: DataFrame): String = {
    val downVotesAndLocation = DataProcessing.selectColumnsWithNames(fileContentParsed, "_Location", "_downVotes", "_displayName")
    val orderedDownVotesAndLocation = DataProcessing.orderDescendingWithColumn(downVotesAndLocation, "_downVotes")
    val downVotesTop = orderedDownVotesAndLocation.first()

    return s"User with highest number of downvotes is: ${downVotesTop.getAs("_displayName")} " +
      s"is located: ${downVotesTop.getAs("_Location")} and has ${downVotesTop.getAs("_downVotes")} down votes!"

  }

  def findTop10Tags(fileContentParsed: DataFrame):Array[Row] = {
    val tags = DataProcessing.selectColumnsWithNames(fileContentParsed, "_Tags")
    val tagsWithoutNullVals = DataProcessing.filterNullValues(tags)

    val mappingFunction: (Row => Traversable[String]) =
      (row => row.getAs[String]("_Tags").replace("<","").split(">").toTraversable)

    val mappedDataFrame = DataProcessing.mapDataSetToStringArray(mappingFunction, tagsWithoutNullVals)
    val dataFrameGrouped = DataProcessing.groupDataFrameByColumn(mappedDataFrame, "tags")
    val dataFrameCounted = DataProcessing.countGroupedValues(dataFrameGrouped)
    val top10Tags = DataProcessing.orderDescendingWithColumn(dataFrameCounted, "count").take(10)

    return top10Tags
  }
}
