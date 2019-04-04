import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Social {

  def main(args: Array[String]){

    //if (args.length == 0) {println("i need two two parameters ")}

    val conf = new SparkConf()
      .setAppName("Social Network")
      .setMaster("local") // remove this when running in a Spark cluster

    val sc = new SparkContext(conf)
    println("Connected to Spark. Running...")

    // Display only ERROR logs in terminal
    sc.setLogLevel("ERROR")

    val users = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

    val likes = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )

    val usersRDD: RDD[(Long, (String, Int))] = sc.parallelize(users) // vertices
    val likesRDD: RDD[Edge[Int]] = sc.parallelize(likes) // edges

    val graph: Graph[(String, Int), Int] = Graph(usersRDD, likesRDD)

    println("List of people who are at least 30 years old:")
    graph.vertices.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    println("Number of triplets in the graph: "+ graph.triplets.count())

    sc.stop()
    println("Disconnected from Spark")

  }

}
