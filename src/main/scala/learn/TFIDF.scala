package learn

import org.apache.spark.{SparkConf, SparkContext}
import math.log10
/**
  * Created by xinmei on 16/5/26.
  */
object TFIDF {

  def main(args:Array[String])={


    val conf = new SparkConf()

    val sc  = new SparkContext(conf)




    val hdfspath = "hdfs:///lxw/tfidf1"
    val savepath1 = "hdfs:///lxw/tf"
    val savepath2 = "hdfs:///lxw/idf"
    val savepath3 = "hdfs:///lxw/N"

    val hadoopConf = sc.hadoopConfiguration




    val awsAccessKeyId = args(0)
    val awsSecretAccessKey = args(1)

    val anchordate = "20160426"

    hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3n.awsAccessKeyId", awsAccessKeyId)

    hadoopConf.set("fs.s3n.awsSecretAccessKey", awsSecretAccessKey)


    val text = sc.textFile(hdfspath)
      .filter{line => line.split("\t").length >= 3}
      .map{line =>

        val item = line.split("\t")

        val abcPattern = "[a-zA-Z]+".r
        val words = item(1)
        val duid = item(2)

        val userword = (abcPattern. findAllIn(words)).mkString(" ")

        if (userword.length == words.length)
        (duid, userword)
        else
          ("", "")


      }



      .filter{x => x._1 != ""}
      .reduceByKey((a:String, b:String) => a + " "+b)
      .cache

    val N = text.count ()


    val tf = text
        .flatMap{case (docId, doc) =>

          doc.split(" ")
              .map{word =>

                ((docId, word), 1)
              }
        }
      .reduceByKey(_+_)
      .map{case ((docID, term), fre) =>

        (docID, (term, fre))
      }
        .groupByKey()



    val idf = text
      .flatMap{case (docId, doc) =>

        doc.split(" ")
          .map{word =>

            ((word,docId),1)

          }
      }.distinct()
     // .reduceByKey(_+_)
        .map{case ((word1, docId1),fre1)=>

          (word1,fre1)
        }

        .reduceByKey(_+_)
      //.sortByKey()
      .map{case (word,fre)=>

      (word,N/fre)
    }



    text.unpersist()

    //    val reduceddata = mappeddata
    //.collect()
    //.foreach(x => println("lixuefei log " + x))
    HDFS.removeFile(savepath1)
    HDFS.removeFile(savepath2)
    HDFS.removeFile(savepath3)

    tf.saveAsTextFile(savepath1)
    idf.saveAsTextFile(savepath3)


    sc.stop()

  }

}
