/**
 * cd C:\Users\mingo\Documents\NetBeansProjects\lab3\target
 * spark-submit --master local[2] --class upf.edu.TwitterStreamingExample lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties
 * spark-submit --master local[2] --class upf.edu.TwitterStreamingExample --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:C:/Users/mingo/Documents/NetBeansProjects/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties
 */
package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Status;
import twitter4j.auth.OAuthAuthorization;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import twitter4j.TwitterException;

public class TwitterStreamingExample {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

       

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(10));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // Print the stream (only 5 tweets)
        stream.print(5);

        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();

    }

}
