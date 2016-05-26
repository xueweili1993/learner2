package learn

import org.apache.spark.{SparkConf, SparkContext}
/**
  * Created by xinmei on 16/5/26.
  */
object TFIDF {

  def main(args:Array[String])={


    val conf = new SparkConf()

    val sc  = new SparkContext(conf)




    val hdfspath = "hdfs:///lxw/tfidf"
    val savepath = "hdfs:///lxw/tf"

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


        (duid, userword)


      }
      .filter{x => x._1 != ""}
      .reduceByKey((a:String, b:String) => a + " "+b)








//    val reduceddata = mappeddata
    //.collect()
    //.foreach(x => println("lixuefei log " + x))
    HDFS.removeFile(savepath)

    text.saveAsTextFile(savepath)

    sc.stop()

  }

}
