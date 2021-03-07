import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

case class Erdos(author1_name:String, author2_name:String)
def parseErdos(str: String): Erdos = {
    val line = str.split(",") 
    Erdos(line(0), line(1))
}
var textRDD = sc.textFile("dblp_coauthorship.csv")
val header = textRDD.first()
textRDD = textRDD.filter(row => row != header)
val erdosRDD = textRDD.map(parseErdos).cache()
val nodes = erdosRDD.flatMap(erdos => Seq(erdos.author1_name)).distinct().zipWithIndex()
nodes.take(10)
val nodesMap = nodes.map { case ((name), id) => (name -> id) }.collect.toMap
val nobody = "nobody"
/*val erdosHimself = vertices.filter { case (name, id) => name == "Paul Erdös"}
erdosHimself.take(1)*/
val connections = erdosRDD.map(erdos => ((erdos.author1_name, erdos.author2_name), 1)).distinct
connections.take(2)
val edges = connections.map {case ((author1_name, author2_name), 1) => Edge(nodesMap(author1_name), nodesMap(author2_name), 1)}
edges.take(3)
val vertices = nodes.map { case (name, id) => (id, name) }
val graph = Graph(vertices, edges, nobody)
graph.vertices.take(3)
graph.edges.take(3)