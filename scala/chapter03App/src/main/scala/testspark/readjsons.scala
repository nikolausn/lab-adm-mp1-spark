package testspark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object readjsons extends App {
  val sc = new SparkContext(master="spark://ontangs-i7.local:7077",appName="practice");
  //val spark = SparkSession.builder().master("local").appName("practice").getOrCreate(); 
  val spark = SparkSession.builder().master("spark://ontangs-i7.local:7077").appName("practice").getOrCreate(); 
  // Read json files itno rdd
  val jsonFiles = sc.wholeTextFiles("/Volumes/HD-500GB/Users/nikolausn/Documents/msimsvn/msimrepo/lab-adm/data/*/*/*/*.json", 3);
  
  // convert jsonrdd into dataframe
  val jsonDF = spark.read.json(jsonFiles.values);
  //jsonDF.printSchema();
  jsonDF.createOrReplaceTempView("musicdb");
  
  /*
   * Schema
   *  |-- get_analysis_sample_rate: long (nullable = true)
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
  
  val artistSong = spark.sql("SELECT get_artist_name,count(get_artist_name) from musicdb GROUP BY get_artist_name");
  artistSong.show();  
  val yearSong = spark.sql("SELECT get_year,count(get_year) from musicdb GROUP BY get_year");
  yearSong.show();  
  
  
}