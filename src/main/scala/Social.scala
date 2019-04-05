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

    // Create nodes/vertices (id, (properties))
    val users = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )

    // Create edges (start, end, properties)
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

    val userInfo = graph.vertices
    println("\nList of people who are at least 30 years old:")
    userInfo.filter { case (id, (name, age)) => age > 30 }.collect.foreach {
      case (id, (name, age)) => println(s"$name is $age")
    }

    val triplets = graph.triplets
    println("\nNumber of Like combinations: "+ triplets.count())

    //triplets.collect.foreach(println)

    println("\nList of likes:")
    for (likes <- triplets.collect) {
      println(s"${likes.srcAttr._1} liked ${likes.dstAttr._1}'s posts ${likes.attr} times")
    }

    println("\nList of Likes where number of likes is more than 5:")
    for (likes <- triplets.filter(t => t.attr > 5).collect) {
      println(s"${likes.srcAttr._1} likes ${likes.dstAttr._1}'s posts ${likes.attr} times")
    }

    println("\nList of the number of users from whom a user received Like")
    val inDegrees: VertexRDD[Int] = graph.inDegrees
    inDegrees.sortBy(_._2,false).foreach(x => println(s"User ${x._1} received Likes from ${x._2} user(s)"))

    println("\nList of the number of users a user provided Like to (i.e. follows)")
    val outDegrees: VertexRDD[Int] = graph.outDegrees
    outDegrees.sortBy(_._2,false).foreach(x => println(s"User ${x._1} Liked (or follows) ${x._2} user(s)"))

    println("\nNumber of triangles around each user: ")
    graph.triangleCount().vertices.foreach(x => println(s"User ${x._1} forms ${x._2} triangle(s)"))

    println("\nNumber of people whose posts user 5 likes and that also like each other's posts: ")
    graph.triangleCount().vertices.filter(x=>x._1==5).foreach(x => println(x._2))

    println("\nList of the oldest follower of each user:")
    val oldestFollower = graph.aggregateMessages[Int](
      edgeContext => edgeContext.
        sendToDst(edgeContext.srcAttr._2),//sendMsg (age), map phase
        (x,y) => math.max(x,y) //mergeMsg (find max), reduce phase
    )
    oldestFollower.sortBy(_._1,true).foreach(x => println(s"User ${x._1}'s oldest follower is ${x._2} years of age"))

    println("\nList of the youngest follower of each user:")
    val youngestFollower = graph.aggregateMessages[Int](
      edgeContext => edgeContext.
        sendToDst(edgeContext.srcAttr._2),//sendMsg (age), map phase
        (x,y) => math.min(x,y) //mergeMsg (find min), reduce phase
    )
    youngestFollower.sortBy(_._1,true).foreach(x => println(s"User ${x._1}'s youngest follower is ${x._2} years of age"))

    println("\n List of the oldest person that each person is following:")
    val oldestFollowee = graph.aggregateMessages[Int](
      edgeContext => edgeContext.
        sendToSrc(edgeContext.dstAttr._2),//sendMsg (age), map phase
        (x,y) => math.max(x,y) //mergeMsg (find max), reduce phase
    )
    oldestFollowee.sortBy(_._1,true).foreach(x => println(s"User ${x._1}'s oldest followee is ${x._2} years of age"))

    println("\n List of the youngest person that each person is following:")
    val youngestFollowee = graph.aggregateMessages[Int](
      edgeContext => edgeContext.
        sendToSrc(edgeContext.dstAttr._2),//sendMsg (age), map phase
        (x,y) => math.min(x,y) //mergeMsg (find min), reduce phase
    )
    youngestFollowee.sortBy(_._1,true).foreach(x => println(s"User ${x._1}'s youngest followee is ${x._2} years of age"))

    println("\nList of each user's PageRank in descending order:")
    val ranks = graph.pageRank(0.1).vertices
    ranks.sortBy(_._2,false).foreach(x => println(s"User ${x._1}'s pagerank is ${x._2}"))

    sc.stop()
    println("\nDisconnected from Spark")

  }

}
