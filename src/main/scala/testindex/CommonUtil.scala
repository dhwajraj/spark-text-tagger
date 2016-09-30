package testindex

import org.apache.spark.SparkContext
import org.apache.commons.lang.StringUtils
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet

object CommonUtil {
  def clientCleaning(s:String): String={
    var st=s
    st =st.replaceAll("[\\`\\~\\!\\@\\#\\$\\%\\^\\&\\*\\(\\)\\-\\+\\=\\-\\{\\}\\[\\]\\;\\:\\'\\\"\\<\\>\\,\\.\\/\\?]", "")
    //println(s+"\t"+st)
    st
  }
  
  def loadCsvAsMap(sc: SparkContext, dictPath:String): HashMap[String,HashSet[String]] = {
    var result = HashMap[String,HashSet[String]]()
    val csvRdd = sc.textFile(dictPath)
    csvRdd.setName("DictPhrasesList")
    var iterator = csvRdd.collect().iterator

    while (iterator.hasNext) {
      var next = iterator.next
      if (next != null && StringUtils.isNotEmpty(next)) {
        var nextArr = next.split("\t")
        if (nextArr.length > 1) {
          var v = HashSet[String]()
          if (result.contains(nextArr(1)))
              v = result(nextArr(1))
          v.add(nextArr(0))
          result.put(nextArr(1), v)
        }
      }
    }
    result
  }
}