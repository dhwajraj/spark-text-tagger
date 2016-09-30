package testindex
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._

object Main {
  
    def main(args: Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.OFF);
  		Logger.getLogger("akka").setLevel(Level.OFF);
  		
      val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("Test_Spark_Lookup1"))
      IndexedRDDLookup.indexedRdd = IndexedRDDLookup.getIndexedRDD(sc, "city_names.txt")
  
      val mapd = IndexedRDDLookup.indexedRdd.multiget(Array("Van Buren Point","Las Vegas","Micrososft","ACE"))
      var t = System.currentTimeMillis()
      val text = "Pittsburgh was the most affordable at $756 a month for a $140,500 house with a minimum salary of $32,390. The others, Cleveland, Cincinnati, Detroit and Atlanta, ranged from $803 to $935 a month.  The costs include principal, interest, taxes and insurance for a median-priced home using a 30-year fixed-rate loan. The loan rates were based on buyers with credit scores of 740 or higher in each area and who put down 20 percent.  In the San Francisco area, your monthly payment on a median priced home would be $3,700."
      IndexedRDDLookup.lookupFromText(text, IndexedRDDLookup.indexedRdd)
      println("Time from main of class "+(System.currentTimeMillis()-t))
    }
}