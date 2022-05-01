/**
 * spark-submit --master local[2] --class upf.edu.TwitterWithState lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties es
 * spark-submit --master local[2] --class upf.edu.TwitterWithState --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:C:/Users/mingo/Documents/NetBeansProjects/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties es
 */
package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

public class TwitterWithState {

    public static void main(String[] args) throws IOException, InterruptedException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter With State");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(30));
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction
                = (values, state) -> {
                    Integer newSum = state.or(0);// add the new values with the previous running count to get the new count                  
                   for (Integer i : values) {
                        newSum += i;
                    }
                    return Optional.of(newSum);
                };
        // create a simpler stream of <user, count> for the given language
        final JavaPairDStream<String, Integer> tweetPerUser = stream
                .filter(s -> s.getLang().equals(language))
                .mapToPair(s -> new Tuple2<String, Integer>(s.getUser().getScreenName(), 1))
                .updateStateByKey(updateFunction);

        // transform to a stream of <userTotal, userName> and get the first 20
        final JavaPairDStream<Integer, String> tweetsCountPerUser = tweetPerUser // IMPLEMENT ME
                .mapToPair(s -> s.swap())
                .transformToPair(rdd -> rdd.sortByKey(false));

        tweetsCountPerUser.print(20);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
