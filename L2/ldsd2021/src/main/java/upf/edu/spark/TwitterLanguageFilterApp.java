/**
 * Comando ejecucion:
 * spark-submit --master local[4] --class upf.edu.spark.TwitterLanguageFilterApp lab2-1.0-SNAPSHOT.jar es ./output-folder C:\Eurovision
 */
package upf.edu.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.Arrays;
import upf.edu.parser.SimplifiedTweet;

public class TwitterLanguageFilterApp {

    public static void main(String[] args) {
        
        String language = args[0];;
        String outputFile = args[1];
        String inputFile = args[2];
        
         System.out.println("\nLanguage: " + language + "\nOutput file: " + outputFile + "\nInput file: " + inputFile + "\n");
         
        SparkConf conf = new SparkConf().setAppName("Filter Tweets");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = sparkContext.textFile(inputFile);
        JavaRDD<SimplifiedTweet> tweets = lines
                .filter(line -> !line.isEmpty()) //Primero filtramos todos los strings que estas vacios (problems NullPointer)
                .map(line -> SimplifiedTweet.fromJson(line)) //JavaRDD cada fila contiene un Optional<SimpleTweet>
                .filter(tweet -> tweet.isPresent()) //Filtramos por los tweet existentes. Descartamos los null
                .map(tweet -> tweet.get()) //Mapeamos de Optional<SimpleTweet> a SimpleTweet 
                .filter(tweet -> tweet.getLanguage().equals(language)); //filtarmos tweets por language
                

        long numberTweets = tweets.count();
         tweets.saveAsTextFile(outputFile);
        System.out.println("\n***************************************************\nNumber of tweets in language:'" + language + "' is " + numberTweets + "\n***************************************************\n");

    }

 
}
