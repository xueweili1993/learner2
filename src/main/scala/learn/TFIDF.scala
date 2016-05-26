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
      /*.flatMap{case line  =>
        //val item  = line.replaceAll(" +", " ").split("\\W+").map(y => (y, 1))
        val item  = line.split("\t")
        val kk= item.map(y=>{(y(3),y(2))})
        kk
      }*/
    val line =text
      .filter{line => line.split("\t").length >= 3}
      .map{line =>

        val item = line.split("\t")

        val words = item(1)
        val duid = item(2)
        (duid, words)
      }
        .reduceByKey((a:String, b:String) => a + " "+b)








//    val reduceddata = mappeddata
    //.collect()
    //.foreach(x => println("lixuefei log " + x))

    text.saveAsTextFile(savepath)

    sc.stop()

  }

}
