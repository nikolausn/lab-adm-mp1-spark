package testspark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import org.apache.spark.sql.functions.explode
import scala.collection.mutable.WrappedArray
import org.apache.hadoop.conf.Configuration
import org.apache.commons.io.FileUtils
import java.io.File

object readjsons extends App {
  // spark local
  //val sc = new SparkContext(master = "local", appName = "practice");
  //val spark = SparkSession.builder().master("local").appName("practice").getOrCreate();
  //val jsonFiles = sc.wholeTextFiles("/Volumes/HD-500GB/Users/nikolausn/Documents/msimsvn/msimrepo/lab-adm/python-converter/data/*/*/*/*.json");

  
  // spark with hadoop
  val sc = new SparkContext(master="spark://sp17-cs511-02.cs.illinois.edu:7077",appName="practice");
  val spark = SparkSession.builder().master("spark://sp17-cs511-02.cs.illinois.edu:7077").appName("practice").getOrCreate(); 
  // Read json files itno rdd
  val jsonFiles = sc.wholeTextFiles("hdfs://sp17-cs511-02.cs.illinois.edu:54310/musicds/*/*/*/*.json");

  // convert jsonrdd into dataframe
  val jsonDF = spark.read.json(jsonFiles.values);
  //jsonDF.printSchema();
  jsonDF.createOrReplaceTempView("musicdb");

  /*
   * Schema for the million song dataset after reading the json files
   * 
 |-- get_analysis_sample_rate: long (nullable = true)
 |-- get_artist_7digitalid: long (nullable = true)
 |-- get_artist_familiarity: double (nullable = true)
 |-- get_artist_hotttnesss: double (nullable = true)
 |-- get_artist_id: string (nullable = true)
 |-- get_artist_latitude: double (nullable = true)
 |-- get_artist_location: string (nullable = true)
 |-- get_artist_longitude: double (nullable = true)
 |-- get_artist_mbid: string (nullable = true)
 |-- get_artist_mbtags: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- get_artist_mbtags_count: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- get_artist_name: string (nullable = true)
 |-- get_artist_playmeid: long (nullable = true)
 |-- get_artist_terms: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- get_artist_terms_freq: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- get_artist_terms_weight: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- get_audio_md5: string (nullable = true)
 |-- get_danceability: double (nullable = true)
 |-- get_duration: double (nullable = true)
 |-- get_end_of_fade_in: double (nullable = true)
 |-- get_energy: double (nullable = true)
 |-- get_key: long (nullable = true)
 |-- get_key_confidence: double (nullable = true)
 |-- get_loudness: double (nullable = true)
 |-- get_mode: long (nullable = true)
 |-- get_mode_confidence: double (nullable = true)
 |-- get_release: string (nullable = true)
 |-- get_release_7digitalid: long (nullable = true)
 |-- get_sections_confidence: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- get_similar_artists: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- get_song_hotttnesss: double (nullable = true)
 |-- get_song_id: string (nullable = true)
 |-- get_start_of_fade_out: double (nullable = true)
 |-- get_tempo: double (nullable = true)
 |-- get_time_signature: long (nullable = true)
 |-- get_time_signature_confidence: double (nullable = true)
 |-- get_title: string (nullable = true)
 |-- get_track_7digitalid: long (nullable = true)
 |-- get_track_id: string (nullable = true)
 |-- get_year: long (nullable = true)
   */
  
  import org.apache.hadoop.fs._
  
  /*
   * method to save distributed file from rddSaveAs into one file
   */
  def merge(srcPath: String, dstPath: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
    /* 
     * delete the src directory after merging
     */
    FileUtils.deleteDirectory(new File(srcPath))
  }
  
  
  val artistSong = spark.sql("SELECT get_year,get_artist_name,count(get_artist_name) as total_music from musicdb GROUP BY get_year,get_artist_name order by get_year desc,total_music desc");
  //artistSong.show();
  artistSong.rdd.saveAsTextFile("artist_song")
  /*
   * save output into one file
   */
  merge("artist_song", "artist_song_output.txt")
  
  
  val artistHotness = spark.sql("SELECT get_artist_name,avg(get_artist_hotttnesss) as avg_hottness,avg(get_duration) as avg_duration from musicdb GROUP BY get_artist_name order by avg_hottness desc");
  //artistHotness.show()
  artistHotness.rdd.saveAsTextFile("artist_hotness")
  merge("artist_hotness","artist_hotness_output.txt")
  


  // third task, counting music terms
  // we can use Spark SQL for this, need to use RDD and map reduce
  // this works like wordcount but the complexity lies in the list of string for every RDD row
  // val termsFlat = artistTerms.rdd.groupBy { x => x }.map( t => (t._1,t._2.size))
  val artistTerms = spark.sql("SELECT get_artist_terms from musicdb");
  //artistTerms.show();
  val termsFlat = artistTerms.rdd
  val first = termsFlat.first()
  val mapped = first.getAs[WrappedArray[Int]](0)
  val test = mapped.mkString("\n")

  // val termsString = termsFlat.map { x => x.getAs[WrappedArray[Int]](0).mkString("\n") }
  // combine the terms
  // after explode the terms for each artist and sign a weight 1 for each terms, we flatten the map function
  // so it will combine all the values in the RDD
  val termsString = termsFlat.map {
    x =>
      x.getAs[WrappedArray[String]](0).groupBy {
        l => l
      }.map(f => (f._1, f._2.length))
  }.flatMap(identity)

  //count the combined terms using reduceByKey, add the count value
  val termsCount = termsString.reduceByKey { case (x, y) => x + y }
  val termsCountSorted = termsCount.map{ case (x,y) => (y,x) }.sortByKey(false);
  termsCountSorted.saveAsTextFile("terms_count")
  merge("terms_count","terms_count_output.txt")

  //print the result
  //println(termsCount.collect().mkString(","))

  //print(termsFlat.first());
  //val termsGroups = artistTerms.rdd.map { x => x.s } groupByKey
  //musicType.rdd

}