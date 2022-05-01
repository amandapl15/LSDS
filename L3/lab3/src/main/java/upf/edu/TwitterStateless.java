/**
 * cd C:\Users\mingo\Documents\NetBeansProjects\lab3\target
 * spark-submit --master local[2] --class upf.edu.TwitterStateless lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties ..\src\main\resources\map.tsv
 * spark-submit --master local[2] --class upf.edu.TwitterStateless --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:C:/Users/mingo/Documents/NetBeansProjects/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties ..\src\main\resources\map.tsv
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
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;
import upf.edu.util.LanguageMapUtils;

import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;

public class TwitterStateless {

    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String input = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Stateless Exercise");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Read the language map file by line
        final JavaRDD<String> languageMapLines = jsc
                .sparkContext()
                .textFile(input);
        // transform it to the expected RDD like in Lab 4
        final JavaPairRDD<String, String> languageMap = LanguageMapUtils
                .buildLanguageMap(languageMapLines);

        // prepare the output
        final JavaPairDStream<String, Integer> languageRankStream = stream
                .mapToPair(s -> new Tuple2<String, Integer>(s.getLang(), 1))
                .reduceByKey((a, b) -> a + b)
                .transformToPair(rdd -> rdd.join(languageMap))
                .mapToPair(s -> new Tuple2<String, Integer>(s._2._2, s._2._1))
                .mapToPair(s -> s.swap())
                .transformToPair(rdd -> rdd.sortByKey(false))
                .mapToPair(s -> s.swap());

        // print first 10 results
        System.out.println("\n**********************************\n");
        languageRankStream.print(10);
        System.out.println("\n**********************************\n");

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
