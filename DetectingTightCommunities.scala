package naveen.bigdata.assignment

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.log4j._
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec._
import org.jgrapht.alg._
import sun.security.provider.certpath.Vertex
import org.jgrapht.graph.DefaultEdge
import breeze.io.TextWriter.FileWriter
import java.io._
import java.util.Scanner
import org.datanucleus.store.types.backed.Vector
import breeze.linalg.DenseVector



object DetectingTightCommunities {
      
  
       case class DataMap(vertexFrom:Long, VertexTo:Long);
       case class VertexRDDEdgeRDD(one:RDD[Edge[Int]],two:RDD[(Long, String)]);
       
       var vertexList  = List[Long]()
       var edgeList = List[DataMap]()
       
       def filterInput(line: String): String = {

       val line1=line.split("\\s+");
        if(! line1(0).toString().equals("#") ){
            return line;
           }
        else
        {
            return null;
        }
       }
       
       def parseInputData(line: String): DataMap = {
         
       if(line==null)
         return null;
       val line1=line.split("\\s+");
       val dataMap:DataMap= DataMap(line1(0).toLong,line1(1).toLong);  
       return dataMap;
       
       }
       
       def max( first: (VertexId,Int), second:( VertexId,Int)) : (VertexId,Int) = 
       {
         var result=first;
         
         if(first._2 < second._2)
           result=second;
        
         return result; 
         
       }
       
       def actTraingleCountFunc(first:(VertexId,(Int,Int))) : (VertexId,Double) =
       {
         var indegree=first._2._2.toDouble;
         var triangleCount=first._2._1.toDouble;
         
         var indegreeC2=  (((indegree) * (indegree-1)) /2).toDouble;
         var result = (triangleCount / (indegreeC2)).toDouble ;
         
         if(indegreeC2==1)
           result=0;
         
         return (first._1,result);
       }
  
       def getVertexToList(first:(Long,String)) =
       {
              vertexList ::=  first._1
       }
       
       def getEdgesToList(first:(Edge[Int])) =
       {
              val dataMap:DataMap= DataMap(first.srcId,first.dstId);
              edgeList ::= dataMap;
       }
       
       def getVertexToList(first:DataMap) =
       {
              vertexList ::=  first.vertexFrom
              vertexList ::=  first.VertexTo
       }
       
       def vectorToMap(first: Array[String])  =
       {
         val test=first.flatMap(x=> Array(x,first))
         test.foreach(println)
       }
       
       def getDataFromFiles(sc: SparkContext) : VertexRDDEdgeRDD = 
       {
           
    //val data = sc.textFile("../Assignment/facebook_combined.txt/facebook_combined.txt");
    var data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/348.edges");
    var dataFilter=data.map(filterInput);
    var input1=dataToEdgesandVectors(dataFilter);
    
    var VertexRdd=input1.two
    var edgesRdd=input1.one
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/0.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/414.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/686.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/698.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/1684.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/1912.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/3437.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/3980.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    data = sc.textFile("../Assignment/facebook.tar/facebook/facebook/107.edges");
    dataFilter=data.map(filterInput);
    input1=dataToEdgesandVectors(dataFilter); 
    
    VertexRdd= VertexRdd.union(input1.two)
    edgesRdd = edgesRdd.union(input1.one)
    
    val sample:VertexRDDEdgeRDD= VertexRDDEdgeRDD(edgesRdd,VertexRdd);  
          
    return sample;
       }
       
       def dataToEdgesandVectors(data: RDD[String]) : VertexRDDEdgeRDD =
       {
          val dataRdd=data.map(parseInputData).filter(x => x!=null);
    
          val VertexRdd2=dataRdd.map(x=> (x.vertexFrom,x.vertexFrom.toString())).filter(x=> (x._1!=null && x._2 !=null)).distinct;
          val VertexRdd1=dataRdd.map(x=> (x.VertexTo,x.VertexTo.toString())).distinct;
    
          val VertexRdd=VertexRdd2.union(VertexRdd1).distinct();
    
          var edgesRdd = dataRdd.map(x => Edge(x.vertexFrom,x.VertexTo,1));
          val reverseEdgesRdd=dataRdd.map(x => Edge(x.VertexTo,x.vertexFrom,1));
    
          edgesRdd=edgesRdd.union(reverseEdgesRdd).distinct();
          
          val sample:VertexRDDEdgeRDD= VertexRDDEdgeRDD(edgesRdd,VertexRdd);  
          
          return sample;
       }

    def main(args: Array[String]) {
      
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sc = new SparkContext("local[*]", "DetectingTightCommunities");
    val input = getDataFromFiles(sc)
    
    var VertexRDD = input.two
    var edgesRdd = input.one
    
    val defaultEdgePoint = "default";
    
    val graph= Graph(VertexRDD,edgesRdd, defaultEdgePoint);
    
    
    //VertexRdd.foreach(getVertexToList);
    edgesRdd.foreach(getEdgesToList);
    edgeList.foreach(getVertexToList);
    

    val graph1 = new org.jgrapht.graph.Multigraph[Long,org.jgrapht.graph.DefaultEdge](classOf[org.jgrapht.graph.DefaultEdge]);
    vertexList.foreach( v=> graph1.addVertex(v));
      for((x) <- edgeList) { try {graph1.addEdge(x.vertexFrom, x.VertexTo)} catch {
      
      case ex:IllegalArgumentException => println("Error",x.vertexFrom,x.VertexTo)
    }}  
    
    
    
    println("running clique");
    val c =  new BronKerboschCliqueFinder(graph1);
    val setVertices = c.getAllMaximalCliques()
    val toListservice=setVertices.toArray()
  
    val file = new File("../sample1.txt")
    val bw = new PrintStream(file)
    
    toListservice.foreach( x=> bw.print(x.toString()+"\n")) 
    bw.close();
        
    val dataOutput = sc.textFile("../sample1.txt");
    val temp1= dataOutput.filter(x=> x!=null).map( x => x.split('[')).map(x=> x(1)).map(x=> x.split(']')).map(x=> x(0))
    val temp2= temp1.map( x=> x.split(','));
    val temp4= temp2.filter(x=> x.size>=3)
    val temp5= temp4.sortBy(x=> x.size)
    val temp6=temp5.flatMap(x => x.map(y => (y.toString().trim,(DenseVector(x).toString()))))
    
    val file1 = new File("../sample2.txt")
    val bw1 = new PrintStream(file1)
    println("done");
    
    val GraphMaxInDegree: (VertexId,Int) = graph.inDegrees.reduce(max);
    val GraphMaxOutDegree: (VertexId,Int) = graph.outDegrees.reduce(max);

    val connectedComponents= graph.connectedComponents().vertices
    
    val res=graph.stronglyConnectedComponents(10).vertices
     

    val vertexIndegree=graph.inDegrees;
    
    var triangleCount = graph.triangleCount().vertices
    
     val joinedRddVertexIndegreeTCount = triangleCount.join(vertexIndegree);

     val output=joinedRddVertexIndegreeTCount.map(actTraingleCountFunc).map(x=> (x._1.toString(),x._2));
     
     
     val joinResults = temp6.join(output)
     
     val seperate1 = joinResults.map( x => (x._2._1,1))
     val seperate2 = joinResults.map( x => (x._2._1,x._2._2))
     
     val reduceSeperate1 = seperate1.reduceByKey(_+_)
     val reduceSeperate2 = seperate2.reduceByKey(_+_)
     
     val joinSeperate = reduceSeperate1.join(reduceSeperate2)
     val finalResult = joinSeperate.map(x=> (x._1.toString(),(x._2._2.toDouble/x._2._1.toDouble)))
     
     
     
     val finalRes=finalResult.sortBy(_._2,false).collect()
     finalRes.foreach(println)
     
     val fileResult = new File("../result.txt")
     val printWriterSample = new PrintWriter(fileResult)
     
     println("printing result to file")
     finalRes.foreach(x=> printWriterSample.write(x._1+" "+x._2+"\n"));
   
     printWriterSample.close()
    }
}
