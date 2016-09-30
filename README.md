# spark-text-tagger

Script to perform dictionary based n-gram text tagging efficiently in Apache Spark
 * Method 1 : for big distributed dictionary using IndexedRDD.(Slightly slow, use only when memory is constraint)
 * Method 2 : for manageable size dictionary using broadcast varibales.