
object SparkMain extends App{
  override def main(args: Array[String]): Unit = {
    val xmlFileUsers = DataProcessing.readDatasetFromXML("users","Users.xml")
    val xmlFileTags = DataProcessing.readDatasetFromXML("posts","Posts.xml")

    val sparkExercise = new SparkExercise()
    val userWithTopUpvotes = sparkExercise.findLocationOfUserWithMaxUpVotes(xmlFileUsers)
    val userWithTopDownVotes = sparkExercise.findLocationOfUserWithMaxDownVotes(xmlFileUsers)
    val top10Tags = sparkExercise.findTop10Tags(xmlFileTags)


    top10Tags.foreach(row => println(s"Tag ${row.getAs("tags")} occures ${row.getAs("count")} times!"))
    println(userWithTopUpvotes)
    println(userWithTopDownVotes)

  }
}
