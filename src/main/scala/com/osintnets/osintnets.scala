//Package name
package com.osintnets

//Dependencies
import java.io.Serializable

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkContext, graphx}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeRDD, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, expr, udf}
import org.apache.spark.sql.functions._
import org.graphframes.GraphFrame
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}


trait Setter{

  val spark:SparkSession = SparkSession.builder
    .appName("Osint Project")
    .master("local[2]")
    .getOrCreate()


  @transient implicit val sc:SparkContext = spark.sparkContext
  @transient implicit val sql:SQLContext = spark.sqlContext
}


class osintnets extends Setter with Serializable {

  val schemaV: StructType = StructType(Seq(StructField("id", StringType), StructField("name", StringType)))
  val schemaE: StructType = StructType(Seq(StructField("src", StringType), StructField("dst", StringType), StructField("relationship", StringType)))

  var V: DataFrame = spark.createDataFrame(sc.emptyRDD[Row], schemaV)
  var E: DataFrame = spark.createDataFrame(sc.emptyRDD[Row], schemaE)

  @transient var GlobalGraph: GraphFrame = GraphFrame(V, E)

  /*globalGraphInit initialises a Graph that has to be broadcasted to
  * the worker nodes in the cluster. Takes parameters as:
  * ver: The vertices DataFrame
  * edg: The edges DataFrame
  * Output: Returns a global Graph
  *  */
  def globalGraphInit(ver: DataFrame, edg: DataFrame): GraphFrame = {
    GlobalGraph = GraphFrame(ver, edg)
    GlobalGraph.persist()
    broadcast(GlobalGraph.edges)
    broadcast(GlobalGraph.vertices)
    GlobalGraph
  }

  def getGXEdgelist(ver: DataFrame, edg: DataFrame): DataFrame = {
    import spark.implicits._
    val gxV: RDD[(VertexId, String)] = ver.rdd.map(_.getAs[String](0)).zipWithIndex.map(_.swap)
    val gxV_toDF = gxV.toDF("id", "node")
    val round1Mapping = edg.join(gxV_toDF, edg("src") === gxV_toDF("node")).withColumnRenamed("id", "srcVID").select("srcVID", "src", "dst", "count");
    val round2Mapping = round1Mapping.join(gxV_toDF, round1Mapping("dst") === gxV_toDF("node")).withColumnRenamed("id", "dstVID").drop("id", "node")
    round2Mapping
  }

  def GraphXGraph(round2Mapping: DataFrame, ver: DataFrame): Graph[String, String] = {
    import spark.implicits._
    var gxV: RDD[(VertexId, String)] = ver.rdd.map(_.getAs[String](0)).zipWithIndex.map(_.swap)
    var gxE: RDD[Edge[String]] = round2Mapping.rdd.map(row => Edge(row.getAs[Long]("srcVID"), row.getAs[Long]("dstVID"), ("count")))
    var GX = Graph(gxV, gxE);
    GX
  }

  /** GraphReader reads the csv file and generate the Graph
   * Inputs:
   * path: Path of the data file
   * */
  def GraphReader(path: String): List[DataFrame] = {

    val df = spark.read.format("csv").option("header", "true").load(path)

    val v1 = df.withColumnRenamed("src", "id").drop("dst", "count");
    val v2 = df.select("dst").withColumnRenamed("dst", "id")
    val v = v1.union(v2)
    List(v, df)
  }

  /** PossibleLinks generates the pair of unconnected links that can be connected in the future.
   * It ignores the links that are already connected.
   * Inputs:
   * dataframe1: Vertices dataframe
   * dataframe2: Edges dataframe
   * Output: PossibleLinks whose Adamic Score is to be found */
  def PossibleLinks(dataframe1: DataFrame, dataframe2: DataFrame): DataFrame = {

    // All Nodes from the edgelist
    val a = dataframe1.withColumnRenamed("id", "srcs")
    val b = dataframe1.withColumnRenamed("id", "dsts")

    val dataframe3 = a.join(b, !(a("srcs") === b("dsts")), "cross").drop("name", "age")

    dataframe3.createOrReplaceTempView("tab1")
    dataframe2.createOrReplaceTempView("tab2")

    val sqlDF = spark.sql("select srcs,dsts from tab1 where not exists(select src,dst from tab2 where tab1.srcs == tab2.src " +
      "AND tab1.dsts == tab2.dst)")

    sqlDF.cache();

    val temptable = sqlDF.withColumnRenamed("dsts", "c1").withColumnRenamed("srcs", "c2")

    // Only occuring once
    val occuring_onlyonce = sqlDF.join(temptable, (sqlDF("srcs") === temptable("c1")) && (sqlDF("dsts") === temptable("c2")), "cross")
      .drop("c2", "c1")

    val hasoccuredonce = sqlDF.except(occuring_onlyonce)

    // reverse duplicate edges
    val duals = sqlDF.join(temptable, (sqlDF("srcs") === temptable("c1")) && (sqlDF("dsts") === temptable("c2")), "cross").drop("c1", "c2")
    val unique_duals = duals.select(least("srcs", "dsts") as "src", greatest("srcs", "dsts") as "dst").dropDuplicates()

    // Links whose prediction is to be made
    var final_edgelist = unique_duals.union(hasoccuredonce)
    sqlDF.unpersist();
    final_edgelist
  }


  // Jaccard Similarity Link Prediction Algorithm:
  /** linkPredictorJC function implements Jaccard Similarity Algorithm under the hood
   * Inputs:
   * GlobalGraph: The Graph
   * node1: Source node
   * node2: Destination node
   * */
  def linkPredictorJC(node1: String, node2: String,node1_neighbors:DataFrame,node2_neighbors:DataFrame,mynode:String): DataFrame = {
    import spark.implicits._

    val rdd = Seq(List())
    val rddDf = rdd.toDF()
    val mode_df1_intersect_df2 = node1_neighbors.intersect(node2_neighbors).count();
    val mode_df1_union_df2 = node1_neighbors.union(node2_neighbors).count();

    val newrddDf = rddDf.withColumn("src", lit(node1)).withColumn("dst", lit(node2))
      .withColumn("intersection", lit(mode_df1_intersect_df2)).withColumn("union", lit(mode_df1_union_df2))
      .withColumn("JC", expr("intersection / union")).drop("value", "intersection", "union")

    // Return dataframe with the jaccard Link Prediction Score
    newrddDf
  }

  def removeNull(df: DataFrame,colname:String): DataFrame ={
    df.na.fill(0,Array(colname))
  }

  /** valuePasserJC passes takes the possible links dataframe and passes
   * each link to the linkPredictorJC algorithm and obtains the Jaccard Similarity
   * Score
   * Input: Possible links dataframe
   * Output: Jaccard Similarity Score for possible links, sorted in decreasing prominency
   * */
  def valuePasserJC(gx:GraphFrame, dataframe: DataFrame, dfnbr:DataFrame ,mynode: String): DataFrame = {
    val schema = StructType(StructField("src", StringType) ::
      StructField("dst", IntegerType) ::
      StructField("JC", LongType) :: Nil)

    var newDf = spark.createDataFrame(sc.emptyRDD[Row], schema)

    import spark.implicits._

    dataframe.cache();
    dfnbr.cache();
    val CommonNeighborofmynode = getNeighbors(dfnbr,mynode)

    dataframe.collect.foreach {
      var node1dataframe:DataFrame = Seq(("")).toDF("src");
      var node2dataframe:DataFrame = Seq(("")).toDF("src");
      row => {
        if(row(0).toString == mynode){
        node1dataframe = CommonNeighborofmynode
      }else{
        node1dataframe = getNeighbors(dfnbr,row(0).toString);
      }
        if(row(1).toString == mynode){
          node2dataframe = CommonNeighborofmynode
        }else{
          node2dataframe = getNeighbors(dfnbr,row(1).toString);
        }
        newDf = newDf.union(linkPredictorJC(row(0).toString, row(1).toString,node1dataframe,node2dataframe,mynode))
      }
    }
    // Sort dataframe based upon decreasing Jaccard Similarity Score
    newDf.orderBy($"JC".desc)
  }


  // Common Neighbor Link Prediction Algorithm:
  /** linkPredictorCN function implements Common Neighbor Algorithm under the hood
   * Inputs:
   * GlobalGraph: The Graph
   * node1: Source node
   * node2: Destination node
   * */
  def linkPredictorCN(node1: String, node2: String,node1_neighbors:DataFrame,node2_neighbors:DataFrame,mynode:String): DataFrame = {

    import spark.implicits._

    val common_neighbours = node1_neighbors.intersect(node2_neighbors)
    val common_neighbours_count = common_neighbours.distinct().count();

    val rdd = Seq(List(""))
    val rddDf = rdd.toDF()

    val newrddDf = rddDf.withColumn("src", lit(node1)).withColumn("dst", lit(node2))
      .withColumn("CN", lit(common_neighbours_count)).drop("value")

    // Return dataframe with the Common Neighbor Prediction Score
    newrddDf
  }

  /** valuePasserCN passes takes the possible links dataframe and passes
   * each link to the linkPredictorCN algorithm and obtains the Common Neighbor
   * Score
   * Input: Possible links dataframe
   * Output: Common Neighbor Score for possible links, sorted in decreasing prominency
   * */
  def valuePasserCN(gx:GraphFrame, dataframe: DataFrame,dfnbr:DataFrame,mynode: String): DataFrame = {

    val schema = StructType(StructField("src", StringType) ::
      StructField("dst", IntegerType) ::
      StructField("CN", LongType) :: Nil)

    var newDf = spark.createDataFrame(sc.emptyRDD[Row], schema)

    import spark.implicits._

    dataframe.cache();
    dfnbr.cache();
    val CommonNeighborofmynode = getNeighbors(dfnbr,mynode)
    dataframe.collect.foreach {
      var node1dataframe:DataFrame = Seq(("")).toDF("src");
      var node2dataframe:DataFrame = Seq(("")).toDF("src");
      row => {
        if(row(0).toString == mynode){
          node1dataframe = CommonNeighborofmynode
        }else{
          node1dataframe = getNeighbors(dfnbr,row(0).toString);
        }

        if(row(1).toString == mynode){
          node2dataframe = CommonNeighborofmynode
        }else{
          node2dataframe = getNeighbors(dfnbr,row(1).toString);
        }
        newDf = newDf.union(linkPredictorCN(row(0).toString, row(1).toString,node1dataframe,node2dataframe,mynode))
      }
    }

    // Sort dataframe based upon decreasing Common Neighbour Score
    newDf.orderBy($"CN".desc)
  }


  // Adamic Adar Link Prediction Algorithm:
  /** linkPredictorRA function implements Resource Alocation Algorithm under the hood
   * Inputs:
   * GlobalGraph: The Graph
   * node1: Source node
   * node2: Destination node
   * */
  def linkPredictorRA(node1: String, node2: String,node1_neighbors:DataFrame,node2_neighbors:DataFrame,computed_deg:DataFrame,mynode:String): DataFrame = {
    import spark.implicits._
    val common_neighbours = node1_neighbors.intersect(node2_neighbors);
    common_neighbours.cache();
    val rddDf = common_neighbours.join(computed_deg, common_neighbours("src") === computed_deg("id")).select("degree").agg(sum("degree")).withColumnRenamed("sum(degree)","value")

    var newrddDf = rddDf.withColumn("src", lit(node1)).withColumn("dst", lit(node2)).withColumn("RA",expr("1 / value")).drop("value", "logval")

    common_neighbours.unpersist();
    newrddDf
  }


  /** valuePasserRA passes takes the possible links dataframe and passes
   * each link to the linkPredictorRA algorithm and obtains the Resource Allocation
   * Score
   * Input: Possible links dataframe
   * Output: Resource Allocation Score for possible links, sorted in decreasing prominency
   * */
  def valuePasserRA(gx:GraphFrame, dataframe: DataFrame,dfnbr:DataFrame,mynode: String): DataFrame = {

    val schema = StructType(StructField("src", StringType) ::
      StructField("dst", IntegerType) ::
      StructField("RA", LongType) :: Nil)

    var newDf = spark.createDataFrame(sc.emptyRDD[Row], schema)

    import spark.implicits._
    val computed_deg = gx.degrees.toDF("id", "degree")

    dataframe.cache();
    dfnbr.cache();
    val CommonNeighborofmynode = getNeighbors(dfnbr,mynode)
    dataframe.collect.foreach {
      var node1dataframe:DataFrame = Seq(("")).toDF("src");
      var node2dataframe:DataFrame = Seq(("")).toDF("src");
      row => {
        if(row(0).toString == mynode){
          node1dataframe = CommonNeighborofmynode
        }else{
          node1dataframe = getNeighbors(dfnbr,row(0).toString);
        }
        if(row(1).toString == mynode){
          node2dataframe = CommonNeighborofmynode
        }else{
          node2dataframe = getNeighbors(dfnbr,row(1).toString);
        }
        newDf = newDf.union(linkPredictorRA(row(0).toString, row(1).toString,node1dataframe,node2dataframe,computed_deg,mynode))
      }
    }
    // Sort dataframe based upon decreasing Resource Allocation Score
    newDf.orderBy($"RA".desc)
  }

  // Adamic Adar Link Prediction Algorithm:
  /** linkPredictorAA function implements Adamic Adar Algorithm under the hood
   * Inputs:
   * GlobalGraph: The Graph
   * node1: Source node
   * node2: Destination node
   * */
  def linkPredictorAA(node1: String, node2: String,node1_neighbors:DataFrame,node2_neighbors:DataFrame,computed_deg:DataFrame,mynode:String): DataFrame = {

    val common_neighbours = node1_neighbors.intersect(node2_neighbors);
    common_neighbours.cache();
    val rddDf = common_neighbours.join(computed_deg, common_neighbours("src") === computed_deg("id")).select("degree").agg(sum("degree")).withColumnRenamed("sum(degree)","value")

    var newrddDf = rddDf.withColumn("src", lit(node1)).withColumn("dst", lit(node2))
    .withColumn("logval", log10(rddDf.col("value"))).withColumn("Adamic_Adar_Score",expr("1 / logval")).drop("value", "logval")

    common_neighbours.unpersist();
    newrddDf

  }


  /** valuePasserAA passes takes the possible links dataframe and passes
   * each link to the linkPredictor algorithm and obtains the Adamic Adar
   * Score
   * Input: Possible links dataframe
   * Output: Adamic Adar Score for possible links, sorted in decreasing prominency
   * */
  def valuePasserAA(gx:GraphFrame, dataframe: DataFrame,dfnbr:DataFrame,mynode: String): DataFrame = {
    import spark.implicits._
    val schema = StructType(StructField("src", StringType) ::
      StructField("dst", IntegerType) ::
      StructField("Adamic_Adar_Score", LongType) :: Nil)


    var newDf = spark.createDataFrame(sc.emptyRDD[Row], schema)

    val computed_deg = gx.degrees.toDF("id", "degree");

    val CommonNeighborofmynode = getNeighbors(dfnbr,mynode)

    dataframe.cache();
    dfnbr.cache();
    dataframe.collect.foreach {
      var node1dataframe:DataFrame = Seq(("")).toDF("src");
      var node2dataframe:DataFrame = Seq(("")).toDF("src");
      row =>{
        if(row(0).toString == mynode){
           node1dataframe = CommonNeighborofmynode
        }else{
           node1dataframe = getNeighbors(dfnbr,row(0).toString);
        }

        if(row(1).toString == mynode){
           node2dataframe = CommonNeighborofmynode
        }else{
           node2dataframe = getNeighbors(dfnbr,row(1).toString);
        }
        newDf = newDf.union(linkPredictorAA(row(0).toString, row(1).toString,node1dataframe,node2dataframe,computed_deg,mynode))
      }
    }
    newDf.orderBy($"Adamic_Adar_Score".desc)

  }

  def GetValue(dataframe:DataFrame):DataFrame = {
   dataframe.select("src","srcVID").union(dataframe.select("dst","dstVID").withColumnRenamed("dst","src").withColumnRenamed("dstVID","srcVID")).distinct()
  }

  def getNeighbors(dataframe:DataFrame,mynode:String):DataFrame = {
    var df1 = dataframe.select("dst").filter(col("src") === mynode)
    df1 = df1.withColumnRenamed("dst","src");

    var df2 = dataframe.select("src").filter(col("dst") === mynode)
    val opdf = df1.union(df2).toDF();
    opdf.distinct()
  }

  def PotentialLinks(dframe:DataFrame,GXEdgelist:DataFrame,mynode:String): DataFrame ={
    val dframeA = dframe.select("src","dst").filter((col("src") === mynode) )
    val dframeB = dframe.select("src","dst").filter((col("dst") === mynode) )
    var dframeTemp1 = dframeA.union(dframeB)
    dframeTemp1 = dframeTemp1.select(least("src", "dst") as "src", greatest("src", "dst") as "dst").dropDuplicates()
    val dframeTemp2 = GXEdgelist.select(least("src", "dst") as "src", greatest("src", "dst") as "dst").dropDuplicates()
    val output = dframeTemp1.except(dframeTemp2)
    output
  }

}
// Main
object osintnets extends App with Setter with Serializable {
  import spark.implicits._
  // Call your methods here
}