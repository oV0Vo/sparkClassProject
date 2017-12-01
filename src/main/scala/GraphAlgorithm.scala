import java.text.SimpleDateFormat

import GraphAlgorithm.MyVertex
import org.apache.spark.graphx._
import org.graphstream.graph.{Graph => GraphStream}
import org.apache.spark.rdd.{PartitionCoalescer, PartitionGroup, RDD}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.graphstream.graph.implementations.{AbstractEdge, MultiGraph, MultiNode, SingleNode}

import scala.util.Random

object GraphAlgorithm {

  //todo 删掉id
//  case class PRVertex(var outDegree:Int, var value: Double, vertexType: Int)

  def uniformGraph(srcGraph: Graph[Double, Double]): Graph[Double, Double] = {
    //todo 商店边的权重需要和用户的差不多
    val normalizedGraph = srcGraph.pregel(0.0, 1)(
      vprog = (id, vAttr, a) => a,
      sendMsg = triplet => Iterator((triplet.srcId, triplet.attr)),
      mergeMsg = (a, b) => a + b
    )
    normalizedGraph.mapTriplets(triplet => triplet.attr / triplet.srcAttr)
  }


  //static converge
  def personalRankUnweighted(graph: Graph[Double, Double], userId: Int, d: Double, convergence: Double = 0.01, maxIters:Int = 15):
      Graph[Double, Double] = {
    val initGraph = graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => {
      val initPR = if (vid != userId) 0.0 else 1.0
      (degOpt.getOrElse(0), initPR)
    })

    //todo convergence condition
    val prGraph = initGraph.pregel(0.0, maxIters)(
      vprog = (id, oldVertex, adjPRSum) => {
        if (id == userId)
          (oldVertex._1, 1 - d + adjPRSum)
        else
          (oldVertex._1, adjPRSum)
      },
      sendMsg = triplet => Iterator((triplet.dstId, d * triplet.srcAttr._2 / triplet.srcAttr._1)),
      mergeMsg = (pr1, pr2) => pr1 + pr2
    )

    prGraph.mapVertices((id, attr) => attr._2)
  }

  //用迭代法算的，用矩阵
  def personalRankWeighted(graph: Graph[Double, Double], userId: Int, d: Double, convergence: Double = 0.01, maxIters:Int = 15):
  Graph[Double, Double] = {
//    init PR
    val initGraph = graph.mapVertices((id, oldVertex) => {
      if (id != userId)
        0.0
      else
        1
    })

    //todo convergence condition
    val prGraph = initGraph.pregel(0.0, maxIters)(
      vprog = (id, oldVertex, adjPRSum) => {
        if (id == userId)
          1 - d + adjPRSum
        else
          adjPRSum
      },
      sendMsg = triplet => Iterator((triplet.dstId, d * triplet.srcAttr * triplet.attr)),
      mergeMsg = (pr1, pr2) => pr1 + pr2
    )

    prGraph
  }


  class Constants {

  }

  class RecordLine(shopId: Long, userId: Long){
    override def toString: String = shopId + " " + userId
  }

  case class EdgeLine(shopId1: Long, shopId2:Long, weight:Double) {
    override def toString: String = shopId1 + " " + shopId2 + " " + weight
  }

  case class TupleLine(a: Int, b: String) {
    override def toString: String = a + "," + b
  }

  class MyVertex()

  case class ShopVertex() extends MyVertex

  case class UserVertex() extends MyVertex

  def caculateAndSaveEdges(sc: SparkContext): Unit = {
    val nowTimestamp = System.currentTimeMillis()
    val timeFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val startTimestamp = timeFormatter.parse("2015-01-01 00:00:00").getTime
    val timeInterval:Double = nowTimestamp - startTimestamp // start point
    //todo 时间衰减函数

    val userViewStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_view.txt", 8)
    val userPayStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_pay.txt", 100)
    val userViewEdges = userViewStream.flatMap(line => {
      val parts = line.split(",")
      val timestamp = timeFormatter.parse(parts(2)).getTime
      var timeDecayFactor = 1 - (timestamp - nowTimestamp) / timeInterval //linear decay
      val weight = 0.5 * timeDecayFactor
      Iterator(((parts(0).toLong, parts(1).toLong), weight), ((parts(1).toLong, parts(0).toLong), weight))
    })
    val userPayEdges = userViewStream.flatMap(line => {
      val parts = line.split(",")
      val timestamp = timeFormatter.parse(parts(2)).getTime
      var timeDecayFactor = 1 - (timestamp - nowTimestamp) / timeInterval //linear decay
      val weight = 1 * timeDecayFactor
      Iterator(((parts(0).toLong, parts(1).toLong), weight), ((parts(1).toLong, parts(0).toLong), weight))
    })
    val reduceUserViewEdges = userViewEdges.union(userPayEdges).reduceByKey(_ + _, 12)
        .map(v => Edge(v._1._1, v._1._2, v._2))
    val graph = Graph.fromEdges(reduceUserViewEdges, 0.0)
    val uniformGraphed = uniformGraph(graph)
    uniformGraphed.edges.map(edge => EdgeLine(edge.srcId, edge.dstId, edge.attr)).saveAsTextFile("hdfs://hdfsmaster:9000/graph/userEdges")

      //.saveAsTextFile("hdfs://hdfsmaster:9000/graph/userViewEdges")
//    val userPayStream = sc.textFile("hdfs://hdfsmaster:9000/graph/user_pay.txt", 128)
//    val userStream = userPayStream.union(userViewStream)
//    val userShops:RDD[(Int, Int)] = userStream.map(line => {
//      val parts = line.split(",")
//      (parts(1).toInt, parts(0).toInt)
//    })
//    val shopInfoStream = sc.textFile("hdfs://hdfsmaster:9000/graph/shop_info.txt")
//    val shopInfos = shopInfoStream.map(line => {val parts = line.split(","); (parts(0).toInt, parts(1))})
//    val userVisitedCitys = userShops.join(shopInfos).map(v => (v._2._1, v._2._2)).reduceByKey(_ + " " + _).map(v => TupleLine(v._1, v._2))
//
//    userVisitedCitys.saveAsTextFile("hdfs://hdfsmaster:9000/graph/user_citys")
  }

  def filterGraph(sc: SparkContext, graph:Graph[Double, Double], userId: Int):Graph[Double, Double] = {
    val userCitys:Set[String] = sc.textFile("hdfs://hdfsmaster:9000/graph/user_citys")
                        .filter(line => line.split(",")(0).toLong == userId).first().split(",")(1).split(" ").toSet
    val userCityBR = sc.broadcast(userCitys)
    val cityShops = sc.textFile("hdfs://hdfsmaster:9000/graph/shop_info.txt")
      .filter(line => userCityBR.value.contains(line.split(",")(1))).map(line => line.split(",")(0).toLong).collect().toSet
    //使用广播变量加快筛选
    val cityShopsBR = sc.broadcast(cityShops)
    val userRelatedGraph = graph.subgraph(vpred = (id, attr) => {
      if (id <= 2000)
        cityShopsBR.value.contains(id)
      else
        true
    })
    userRelatedGraph
  }

  def buildGraph2(sc: SparkContext, fileName:String): Graph[Double, Double] = {
    val edgeStream = sc.textFile(fileName)
    val edgeRdd = edgeStream.map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toLong, parts(1).toLong, parts(2).toDouble)
    })
    val graph: Graph[Double, Double] = Graph.fromEdges(edgeRdd, 0.0)
    graph
  }

  def buildGraph(sc: SparkContext): Graph[Double, Double] = {
    val shopStream = sc.textFile("hdfs://hdfsmaster:9000/graph/result/shopEdges")
    val shopEdges = shopStream.map(line => {
         val parts = line.split(" ")
         Edge(parts(0).toLong, parts(1).toLong, parts(2).toDouble)
       })
//    println("shopEdges:" + shopEdges.count)

    val userViewStream = sc.textFile("hdfs://hdfsmaster:9000/graph/result/userViewEdges", 6)
    val userViewEdges = userViewStream.map(line => {
        val parts = line.split(" ")
        Edge(parts(0).toLong, parts(1).toLong, parts(2).toDouble)
      })
//    println("userViewEdges:" + userViewEdges.count())

    val userPayStream = sc.textFile("hdfs://hdfsmaster:9000/graph/result/userPayEdges", 124)
    val userPayEdges = userPayStream.map(line => {
      val parts = line.split(" ")
      Edge(parts(0).toLong, parts(1).toLong, parts(2).toDouble)
    })
//    println("userPayEdges:" + userPayEdges.count())

    val allEdges = shopEdges.union(userViewEdges).union(userPayEdges)
    val graph: Graph[Double, Double] = Graph.fromEdges(allEdges, 0.0)
    graph.partitionBy(PartitionStrategy.RandomVertexCut)
//    graph.groupEdges(_ + _)
//    graph.edges.saveAsTextFile("hdfs://hdfsmaster:9000/graph/result/edges")
//    println(graph.edges.count())
    graph
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("GraphStreamDemo")
      .set("spark.master", "spark://sparkmaster:7077")

    val userId = args(0).toInt
    val weighted = args(1).toBoolean
    val d = if (args.length >= 3) args(2).toDouble else 0.85;
    val maxIters = if(args.length >= 4) args(3).toInt else 15
    val sc = new SparkContext(sparkConf)
//    caculateAndSaveEdges(sc)
    val graph = buildGraph2(sc, "hdfs://hdfsmaster:9000/graph/userEdges")

    //筛选用户吃过的店铺所在城市的子图
    val userSubGraph = filterGraph(sc, graph, userId)

    //计算PR
    val prGraph = if (weighted) personalRankWeighted(userSubGraph, userId, d, maxIters = maxIters)
                  else personalRankUnweighted(userSubGraph, userId, d, maxIters = maxIters)

    //筛选出店铺节点，选择PR值最高的前十个店铺并打印
//    val shopIds = sc.textFile("hdfs://hdfsmaster:9000/graph/shop_info.txt")
//      .map(line => (line.split(",")(0).toLong, 0))
    val shopVertices = prGraph.vertices.filter(v => v._1 < 2000).map(v => (v._2, v._1))
    val topShopWithPR:RDD[(Double, Long)] = shopVertices.sortByKey(false)
    topShopWithPR.take(10).foreach(println(_))
  }
}
