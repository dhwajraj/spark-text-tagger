

package testindex

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import util.control.Breaks._

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object IndexedRDDLookup {
    
  var indexedRdd: IndexedRDD[String, HashSet[String]] = _
  
  def getIndexedRDD(sc: SparkContext, dictFile: String)={
    val csv = sc.textFile(dictFile)
    val data = csv.map(line => line.split("\t").map(_.trim))
    val pairs = data.flatMap(row=> {
      var arr:List[Tuple2[String,String]] = List()
      val tup = Tuple2(CommonUtil.clientCleaning(row(1)),row(0))
      arr ::= tup
      
      arr
    })
    
    //Same text phrase can belong to multiple keys e.g. Springfield, Memphis
    val groups=pairs.groupByKey().map(pair=>{
      var a = new HashSet[String]()
      a++=pair._2
      (pair._1,a)
    })
    //groups.foreach(println)

    val indexedkv=IndexedRDD(groups)
    indexedkv.cache().foreachPartition(x => {})
    //heavy operation: println(SizeEstimator.estimate(indexedkv))
    indexedkv
  }
  

  def lookupFromText(text:String, indexedRdd: IndexedRDD[String, HashSet[String]]):HashMap[String,HashSet[String]]={
    var result = new HashMap[String,HashSet[String]]()
    var tokens = CommonUtil.clientCleaning(text).split(" ")
    var len=tokens.length-1
    var windowLen=len
    if(windowLen>10)
      windowLen=10
    var continueI=(-1)
    for(i<-(0 to len)){
      if(i>=continueI){
      breakable{for(j<-(i to i+windowLen)){
        var candidate= tokens.slice(i, j).mkString(" ")
        //println(candidate)
        val mapd = indexedRdd.get(candidate)
        if(mapd!=null && !mapd.isEmpty){
          var value=mapd.get
          if(result.contains(candidate))
            value++= result.getOrElse(candidate, null)
          result.put(candidate, value)
          continueI=j
          break
        }
          
      }}
      }
    }
    println()
    println(result)
    result
  }
  

}