package pageRank
import org.apache.spark.{SparkContext,SparkConf} 
import SparkContext._ 
 
/**
 *Created by Wangcao on 3/9/16.
 *
 *计算每个url的评分，根据评分筛选出最优的链接
 */
  
object PageRank { 
 
  def main(args: Array[String]) { 
  
    val conf = new SparkConf()
            .setAppName("pageRank")
            .setMaster("local")
    val sc = new SparkContext(conf) 
		
    //read data
    val lines = sc.textFile("/user/root/316/pg_data/total.txt")  
		
    //create edges
    val links = lines.map(_.split(" ")).filter(_.length == 2).map(parts => (parts(0), parts(1))).distinct().groupByKey()  
    val nodes = scala.collection.mutable.ArrayBuffer.empty ++ links.keys.collect()
    val newNodes = scala.collection.mutable.ArrayBuffer[String]()  
    for {s <- links.values.collect()  
            k <- s if (!nodes.contains(k))  
        } {  
            nodes += k  
            newNodes += k  
        }  
    val linkList = links ++ sc.parallelize(for (i <- newNodes) yield (i, List.empty))  
    val nodeSize = linkList.count() 

    //set initial rank		
    var ranks = linkList.mapValues(v => 1.0 ) 
  
    //execute cycle and calculate final scores for each url
    for (i <- 1 to 30) {  
      val score = sc.accumulator(0.0)  
        val contribut = linkList.join(ranks).values.flatMap {  
          case (urls, rank) => {  
            val size = urls.size  
              if (size == 0) {  
                score += rank  
                   List()  
                     } else {  
                       urls.map(url => (url, rank / size))  
                    }  
                }  
            }      
      
    val scoreValue = score.value  
    ranks = contribut.reduceByKey(_ + _).mapValues[Double](p =>  
      0.1 * (1.0 / nodeSize) + 0.9 * (scoreValue / nodeSize + p)  
      )  
    }  
		
    //ordered by scores and save as text file
    var outputSort = sc.parallelize(ranks.collect())
    outputSort = outputSort.sortBy(word => word._2,false)
    outputSort.saveAsTextFile("/user/root/316/result2/rank_total")
		
    sc.stop() 
    }  
}  