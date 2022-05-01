/**
 * spark-submit --master local[2] --class upf.edu.TwitterWithWindow lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties ..\src\main\resources\map.tsv
 * spark-submit --master local[2] --class upf.edu.TwitterWithWindow --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:C:/Users/mingo/Documents/NetBeansProjects/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties ..\src\main\resources\map.tsv
 */
package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;
import scala.Tuple2;

public class TwitterWithWindow {

    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter with windows");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file as RDD
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);

        // create an initial stream that counts language within the batch (as in the previous exercise)
        final JavaPairDStream<String, Integer> languageCountStream = stream
                .mapToPair(s -> new Tuple2<String, Integer>(s.getLang(), 1))
                .reduceByKey((a, b) -> a + b)
                .transformToPair(rdd -> rdd.join(languageMap))
                .mapToPair(s -> new Tuple2<String, Integer>(s._2._2, s._2._1));

        // Prepare output within the batch
        final JavaPairDStream<Integer, String> languageBatchByCount = languageCountStream
                .mapToPair(s -> s.swap())
                .transformToPair(rdd -> rdd.sortByKey(false));

        // Prepare output within the window
        final JavaPairDStream<Integer, String> languageWindowByCount = languageCountStream
                .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(60))
                .mapToPair(s -> s.swap())
                .transformToPair(rdd -> rdd.sortByKey(false));

        // Print first 15 results for each one          
        languageBatchByCount.print();      
        languageWindowByCount.print();
    
        
        

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
