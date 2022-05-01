/**
 * cd C:\Users\mingo\Documents\NetBeansProjects\lab3\target
 * spark-submit --master local[2] --class upf.edu.TwitterHashtags lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties
 * spark-submit --master local[2] --class upf.edu.TwitterHashtags --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:C:/Users/mingo/Documents/NetBeansProjects/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties
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
import upf.edu.storage.DynamoHashTagRepository;

public class TwitterHashtags {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        SparkConf conf = new SparkConf().setAppName("Real-time Twitter Example");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(5));
        // This is needed by spark to write down temporary data
        jsc.checkpoint("/tmp/checkpoint");

        final JavaReceiverInputDStream<Status> stream = TwitterUtils.createStream(jsc, auth);

        // <IMPLEMENT ME>
        DynamoHashTagRepository dHash = new DynamoHashTagRepository();
        dHash.createTable();//Creamos tabla si no existe
        stream.foreachRDD(s -> s.foreach(tweet -> dHash.write(tweet)));
        // Start the application and wait for termination signal
        jsc.start();
        jsc.awaitTermination();
    }
}
