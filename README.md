# spark-text-tagger

Script to perform dictionary based n-gram text tagging efficiently in Apache Spark
 * Method 1 : for big distributed dictionary using IndexedRDD.(Slightly slow, use only when memory is constraint and dict is too big)
 * Method 2 : for manageable size dictionary which could fit in worker memory, using broadcast varibales (shared by multiple workers per machine).