import java.text.SimpleDateFormat
import java.util.Scanner

import GraphAlgorithm.uniformGraph
import org.apache.spark.graphx.{Edge, Graph, VertexRDD}
import org.apache.spark.mllib.clustering.PowerIterationClustering
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Clustering {

  case class EdgeLine(u1:Long, u2:Long, v:Double) {
    override def toString: String = u1 + " " + u2 + " " + v
  }

  def uniformGraph(srcGraph: Graph[Double, Double]): Graph[Double, Double] = {
    //todo 商店边的权重需要和用户的差不多
    val normalizedGraph = srcGraph.pregel(0.0, 1)(
      vprog = (id, vAttr, a) => a,
      sendMsg = triplet => Iterator((triplet.srcId, triplet.attr)),
      mergeMsg = (a, b) => a + b
    )
    normalizedGraph.mapTriplets(triplet => triplet.attr / triplet.srcAttr)
  }

  def buildSimularityGraph(sc:SparkContext, cityName:String):Unit = {
    val cityShops = sc.textFile("hdfs://hdfsmaster:9000/graph/shop_info.txt")
      .filter(line => cityName.equals(line.split(",")(1))).map(line => line.split(",")(0).toLong).collect().toSet
    //使用广播变量加快筛选
    val cityShopsBR = sc.broadcast(cityShops)

    val nowTimestamp = System.currentTimeMillis()
    val timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startTimestamp = timeFormatter.parse("2015-01-01 00:00:00").getTime
    val timeInterval:Double = nowTimestamp - startTimestamp // start point
    //todo 时间衰减函数

    val userViewStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_view.txt", 8)
    val userPayStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_pay.txt", 100)
    val filterFunc = (line:String) => {
      cityShopsBR.value.contains(line.split(",")(1).toLong)
    }
    val userViewEdges = userViewStream.filter(filterFunc).map(line => {
      val parts = line.split(",")
      val timestamp = timeFormatter.parse(parts(2)).getTime
      var timeDecayFactor = 1 - (timestamp - nowTimestamp) / timeInterval //linear decay
      val weight = 0.5 * timeDecayFactor
      ((parts(0).toInt, parts(1).toInt), weight)
    })

    val userPayEdges = userViewStream.filter(filterFunc).map(line => {
      val parts = line.split(",")
      val timestamp = timeFormatter.parse(parts(2)).getTime
      var timeDecayFactor = 1 - (timestamp - nowTimestamp) / timeInterval //linear decay
      val weight = 1 * timeDecayFactor
      ((parts(0).toInt, parts(1).toInt), weight)
    })

    val mergedUserEdges = userViewEdges.union(userPayEdges).reduceByKey(_ + _, 3)
      .map(v => Edge(v._1._1, v._1._2, v._2))

    val graph = Graph.fromEdges(mergedUserEdges, 0.0)
    val uniformGraphed = uniformGraph(graph)
    uniformGraphed.cache()
    val maxWeightedEdge = uniformGraphed.edges.map(edge => (edge.dstId, (edge.srcId, edge.attr))).reduceByKey((v1, v2) => {
      if (v1._2 > v2._2)
        v1
      else
        v2
    }).collect().toMap
    val maxWeightedBR = sc.broadcast(maxWeightedEdge)
    uniformGraphed.edges.flatMap(edge => {
      val shopMaxWeighted = maxWeightedBR.value(edge.dstId)
      val v = edge.attr * shopMaxWeighted._2
      Iterator(EdgeLine(edge.srcId, shopMaxWeighted._1, v), EdgeLine(shopMaxWeighted._1, edge.srcId, v))
    }).filter(v => v.u1 != v.u2).saveAsTextFile("hdfs://hdfsmaster:9000/graph/userEdges2")
//    val biGraph = Graph.fromEdges(uniformGraphed.edges.union(uniformGraphed.reverse.edges), 0.0)
//    biGraph.edges.map(edge => EdgeLine(edge.srcId, edge.dstId, edge.attr)).saveAsTextFile("hdfs://hdfsmaster:9000/graph/userNormEdges")
  }

  def buildGraphFromFile(sc: SparkContext):Graph[Double, Double] = {
    val graphEdges = sc.textFile("hdfs://hdfsmaster:9000/graph/userNormEdges").map(line => {
      val parts = line.split(" ");
      Edge(parts(0).toLong, parts(1).toLong, parts(2).toDouble)
    })
    val graph = Graph.fromEdges(graphEdges, 0.0)
    val uniformGraphed = uniformGraph(graph)
    uniformGraphed
  }

  def caculateShortestPath(graph:Graph[Int, Double], srcId:Long):VertexRDD[Double] = {
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == srcId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => {  // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    sssp.vertices
  }

  def evaluateSilhouetteCoff(graph:Graph[Int, Double], samples:Array[(Int, Long)]):Unit = {
    val inverseGraph = graph.mapEdges(edgeValue => 1.0 / edgeValue.attr)
    inverseGraph.cache()
    //question:如果是一个rdd想要同时使用其他rdd的所有数据该怎么办
    val coffs = samples.map(v => {
      val sampleClusterId = v._1
      val sampleId = v._2
      val shortestPath = caculateShortestPath(inverseGraph, sampleId)
      //distance:（类间距b，类内距a）
      var distance = inverseGraph.vertices.join(shortestPath).map(v => (v._2._1, (v._2._2, 1)))
        .reduceByKey((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)).map(v => (v._1, v._2._1 / v._2._2)).collect().toMap
//      distance.cache()
	    var a:Double = distance(sampleClusterId)
      distance += (sampleClusterId -> Double.PositiveInfinity)
      var b:Double = distance.values.min
      ((b - a) / Math.max(a, b), 1)
    })
    coffs.foreach(println(_))
    val resultPair = coffs.reduce((v1, v2) => (v1._1 + v2._1, v1._2 + v2._2))
    println("mean coff:" + (resultPair._1 / resultPair._2))
  }

  case class Sample()

  def main(args:Array[String]):Unit = {
    val sparkConf = new SparkConf()
      .setAppName("GraphStreamDemo")
      .set("spark.master", "spark://sparkmaster:7077")
    val sc = new SparkContext(sparkConf)

//    buildSimularityGraph(sc, "南京")
    val graph = buildGraphFromFile(sc)
//    val testEdges = sc.parallelize(Array(Edge(1L, 2L, 0.7), Edge(2L, 1L, 0.7), Edge(3L, 2L, 0.2), Edge(2L, 3L, 2.0),
//                    Edge(3L, 4L, 0.8), Edge(4L, 3L, 0.8), Edge(1L, 4L, 0.2), Edge(4L, 1L, 0.2)))
//    val graph = Graph.fromEdges(testEdges, 0.0)
    graph.cache()

//    for (k <- 12 to 18)
//    {
//val scanner = new Scanner(System.in)
//    while(true) {
//      val nextStr = scanner.nextLine()
//      if (nextStr.equals("exit"))
//        System.exit(0)
      val k = args(0).toInt
      var timestart = System.currentTimeMillis()
      val pic = new PowerIterationClustering()
          .setK(k)
          .setMaxIterations(10)
          .setInitializationMode("random")
      val model = pic.run(graph)
      val assignments = model.assignments.map(v => (v.id, v.cluster))

//    model.assignments.map(v => (v.cluster, v.id))
//     .sample(true, 0.05).saveAsTextFile("hdfs://hdfsmaster:9000/graph/clusterSample")

      val clusterGraph = Graph(assignments, graph.edges)
      val accum = sc.doubleAccumulator("My Accumulator")
      clusterGraph.triplets.foreach(triplet => {
        if (triplet.srcAttr != triplet.dstAttr)
          accum.add(triplet.attr)
      })
      println(accum.value)
//    }
//    scanner.close()
//    }

  }
}
