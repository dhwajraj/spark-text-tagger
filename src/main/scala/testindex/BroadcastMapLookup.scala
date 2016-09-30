package testindex

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import util.control.Breaks._
import org.apache.spark.broadcast.Broadcast

object BroadcastMapLookup {
  
  def lookupFromText(text:String, dictMap:Broadcast[HashMap[String,HashSet[String]]]):HashMap[String,HashSet[String]]={
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
        val mapd = dictMap.value.get(candidate)
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