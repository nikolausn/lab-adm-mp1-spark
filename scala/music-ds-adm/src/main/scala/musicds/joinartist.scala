package musicds

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.WrappedArray

object joinartist extends App {
  // spark local
  val sc = new SparkContext(master = "local", appName = "practice");
  val spark = SparkSession.builder().master("local").appName("practice").getOrCreate();
  //val jsonFiles = sc.wholeTextFiles("/Volumes/HD-500GB/Users/nikolausn/Documents/msimsvn/msimrepo/lab-adm/python-converter/data/*/*/*/*.json");
  val jsonFiles = sc.wholeTextFiles("/Volumes/HD-500GB/Users/nikolausn/Documents/msimsvn/msimrepo/lab-adm/python-converter/output.json");
  val jsonDF = spark.read.json(jsonFiles.values);
  //val jsonDF = spark.read.json("/Volumes/HD-500GB/Users/nikolausn/Documents/msimsvn/msimrepo/lab-adm/python-converter/output.json");
  //jsonDF.printSchema();
  jsonDF.createOrReplaceTempView("musicdb");

  val similarArtist = spark.sql("SELECT distinct get_artist_name,get_artist_id,get_similar_artists from musicdb");
  val similarFlat = similarArtist.rdd
  val artist2 = similarArtist.rdd
  val collector = similarFlat.map { x => (x.get(0), x.getAs[WrappedArray[String]](2).map { x => x }) }
  
}