import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.cql.CassandraConnector

val ssc =  new StreamingContext(sc, Seconds(1))
val lines = ssc.socketTextStream( "localhost", 9999)
val words = lines.flatMap(_.split( " "))
val pairs = words.map(word => (word, 1))
val wordCounts = pairs.reduceByKey(_ + _)
wordCounts.saveToCassandra( "streaming_test",  "words_table", SomeColumns( "word",  "count"))
wordCounts.print()
ssc.start()
ssc.awaitTermination()
