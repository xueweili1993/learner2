package learn

import org.apache.spark.{SparkConf, SparkContext}
import math.log10
import scala.collection.mutable.HashSet
/**
  * Created by xinmei on 16/5/26.
  */
object TFIDF {

  def main(args:Array[String])={


    val conf = new SparkConf()

    val sc  = new SparkContext(conf)




    val stopwordPath = "s3n://xinmei-dataanalysis/ref/stopwords.dict"




    val hdfspath = "hdfs:///lxw/teststop"
    //val savepath1 = "hdfs:///lxw/tfIdf"
    val savepath2 = "hdfs:///lxw/idf"
    val savepath3 = "hdfs:///lxw/stopword"
    val savepath4 = "hdfs:///lxw/origin"

    val hadoopConf = sc.hadoopConfiguration




    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)



    val stop = sc.textFile(stopwordPath)
      .map{line =>

        val word = line.trim

        word
      }.collect()





    val stop_bc = sc.broadcast(stop)

    val text = sc.textFile(hdfspath)
      .filter{line => line.split("\t").length >= 3}
      .map{line =>

        val item = line.split("\t")

        //val abcPattern = "[a-zA-Z]+".r
        val words = item(1)
        val duid = item(2)

        //val userword = (abcPattern. findAllIn(words)).mkString(" ")
        (duid, words)
      }
      .reduceByKey((a:String, b:String) => a + " "+b)
      .cache

    //val N = text.count ()

     val filtered = text
      .flatMap{case (docId, doc)=>

          doc.split(" ")
            . map{word =>
               val abcPattern = "[a-zA-Z]+".r
               val userword = (abcPattern. findAllIn(word)).mkString(" ")
              (word, userword, docId)
            }
      }
      .filter{case (term, myterm,docId)=>
      term.length==myterm.length
      }
      .filter{case (term, myterm,docId) =>
        val haha= stop_bc.value
        !haha.contains(term)
      }
      .map{case (term, myterm,docId)=>

        (docId, term)
      }
      .reduceByKey((a:String, b:String) => a + " "+b)
      .cache

     text.unpersist()
    val N = filtered.count ()

    val idf = filtered
      .flatMap{ case (docId, doc)=>

          doc.split (" ")
            .map {word =>

              ((word, docId),1)
            }

      }
      .distinct()
      //.reduceByKey(_+_)//   after distinct no use
      .map{case ((word, docId),1)=>

      (word,1)
    }
      .reduceByKey(_+_)
      .map{case (word, fre)=>

        (word, log10(N/fre))
      }

   /* val idf = text
      .flatMap{case (docId, doc) =>
        doc.split(" ")
          .map{word =>

            ((word,docId),1)

          }
      }
        .filter { case ((term, docid), cc) =>
            !stop.contains(term)
        }

      .distinct()
      //.reduceByKey(_+_)
        .map{case ((word1, docId1),fre1)=>
          (word1,fre1)
        }

        .reduceByKey(_+_)
      //.sortByKey()
      .map{case (word,fre)=>

      (word,log10(N/fre))
    }

    val tfIdf = text
      .flatMap{case (docId, doc) =>

        doc.split(" ")
          .map{word =>

            ((docId, word), 1)
          }
      }
      .reduceByKey(_+_)
      .map{case ((docID, term), fre) =>

        (term, (docID, fre))
      }
      .join(idf)

      .map{case(term2,((id,tff),z)) =>

        (id,(term2, tff * z))


      }.groupByKey()
      .map{case (id,x) =>

          val y = x.toArray.sortWith( _._2 > _._2)
        (id, y.mkString("\t"))
    }






    text.unpersist() */


    //HDFS.removeFile(savepath1)
    HDFS.removeFile(savepath2)
    HDFS.removeFile(savepath3)
    HDFS.removeFile(savepath4)

    //tfIdf.repartition(1).saveAsTextFile(savepath1)
    idf.saveAsTextFile(savepath2)

    text.saveAsTextFile(savepath3)
    filtered.saveAsTextFile(savepath4)
    sc.stop()

  }

}
