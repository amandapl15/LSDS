/**
 * Comando ejecucion:
 * spark-submit --master local[4] --class upf.edu.spark.MostRetweetedApp lab2-1.0-SNAPSHOT.jar ./output-folder C:\Eurovision
 */
package upf.edu.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import upf.edu.parser.ExtendedSimplifiedTweet;

public class MostRetweetedApp {

    public static void main(String[] args) {

        String outputFile = args[0];
        String inputFile = args[1];

        System.out.println("\nOutput file: " + outputFile + "\nInput file: " + inputFile + "\n");

        SparkConf conf = new SparkConf().setAppName("Filter Tweets").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = sparkContext.textFile(inputFile);

        //Usuarios mas retweeteados
        JavaPairRDD<Long, Integer> usersRetweeted = lines
                .filter(line -> !line.isEmpty()) //Primero filtramos todos los strings que estas vacios (problems NullPointer)
                .map(line -> ExtendedSimplifiedTweet.fromJson(line)) //JavaRDD cada fila contiene un Optional<SimpleTweet>
                .filter(tweet -> tweet.isPresent()) //Filtramos por los tweet existentes. Descartamos los null
                .map(tweet -> tweet.get()) //Mapeamos de Optional<SimpleTweet> a SimpleTweet
                .filter(tweet -> tweet.isIsRetweeted())//filtramos por tweets retuiteados
                .map(tweet -> tweet.getRetweetedUserId())//Devuelve long userId retuiteados 
                .mapToPair(reUserId -> new Tuple2<>(reUserId, 1))
                .reduceByKey((a, b) -> a + b);
        //Tweets mas retweeteados
        JavaPairRDD<Long, Integer> tweetsRetweeted = lines
                .filter(line -> !line.isEmpty()) //Primero filtramos todos los strings que estas vacios (problems NullPointer)
                .map(line -> ExtendedSimplifiedTweet.fromJson(line)) //JavaRDD cada fila contiene un Optional<SimpleTweet>
                .filter(tweet -> tweet.isPresent()) //Filtramos por los tweet existentes. Descartamos los null
                .map(tweet -> tweet.get()) //Mapeamos de Optional<SimpleTweet> a SimpleTweet
                .filter(tweet -> tweet.isIsRetweeted())//filtramos por tweets retuiteados
                .map(tweet -> tweet.getRetweetedTweetId())//Devuelve long Id tweets retuiteados 
                .mapToPair(ReTweetId -> new Tuple2<>(ReTweetId, 1))
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, Long> usersRetweetedOrder = orderByValue(usersRetweeted, sparkContext); //Ordena por valor
        JavaPairRDD<Integer, Long> tweetsRetweetedOrder = orderByValue(tweetsRetweeted, sparkContext); //Ordena por valor
        printMostPopular(usersRetweetedOrder, 1);//Printa los 10 users mas retweeteados
        System.out.println("\n***************************\nNumber of retweeted users: " + usersRetweeted.count() + "\n***************************\n");
        printMostPopular(tweetsRetweetedOrder, 2);//Printa los 10 tweets mas retweeteados
        System.out.println("\n***************************\nNumber of retweeted tweets: " + tweetsRetweeted.count() + "\n***************************\n");

        usersMostRetweeted(usersRetweetedOrder, lines, sparkContext);//Fisrt 10 users with tweet most popular

        usersRetweeted.saveAsTextFile(outputFile);

    }

    //Print Fisrt 10 users with tweet most popular
    private static void usersMostRetweeted(JavaPairRDD<Integer, Long> users, JavaRDD<String> lines, JavaSparkContext sparkContext) {

        ArrayList<Tuple2<Long, Long>> input = new ArrayList();
        int max = 0;
        //Por cada usuario lista el mas retuiteado
        for (Tuple2<Integer, Long> user : users.collect()) {
            if (max < 10) {

                Long idUser = (Long) user._2();
                int key = (Integer) user._1();

                JavaPairRDD<Long, Integer> tweetsRetweeted = lines
                        .filter(line -> !line.isEmpty()) //Primero filtramos todos los strings que estas vacios (problems NullPointer)
                        .map(line -> ExtendedSimplifiedTweet.fromJson(line)) //JavaRDD cada fila contiene un Optional<SimpleTweet>
                        .filter(tweet -> tweet.isPresent()) //Filtramos por los tweet existentes. Descartamos los null
                        .map(tweet -> tweet.get()) //Mapeamos de Optional<SimpleTweet> a SimpleTweet
                        .filter(tweet -> tweet.isIsRetweeted())//filtramos por tweets retuiteados
                        .filter(tweet -> tweet.getRetweetedUserId().equals(idUser)) //Si el usuario es el iterado
                        .map(tweet -> tweet.getRetweetedTweetId())//Devuelve long Id tweets retuiteados 
                        .mapToPair(ReTweetId -> new Tuple2<>(ReTweetId, 1))
                        .reduceByKey((a, b) -> a + b);

                JavaPairRDD<Integer, Long> orderTweet = orderByValue(tweetsRetweeted, sparkContext);//Ordena tweets
                Tuple2<Integer, Long> idTweet = orderTweet.first();//Coje el primero
                input.add(new Tuple2<>(idUser, idTweet._2)); //Añadimos userId y idTweet

            } else {
                break;
            }
            max++;
        }

        System.out.println("\n******** The most retweeted tweet for the 10 most retweeted users *********\n");
        int cont = 1;
        for (Tuple2<Long, Long> tuple : input) {
            System.out.println("Id User " + (cont++) + " : " + tuple._1() + " - Id Tweet: " + tuple._2());
        }
        System.out.println("\n*************************************************\n");
    }
    //Ordena JavaPairRDD por valor
    private static JavaPairRDD<Integer, Long> orderByValue(JavaPairRDD<Long, Integer> word, JavaSparkContext sparkContext) {

        List<Tuple2<Integer, Long>> input = new ArrayList<>();
        //Intercambiamos key, value
        for (Tuple2<Long, Integer> tuple : word.collect()) {
            int value = (Integer) tuple._2();
            Long key = (Long) tuple._1();
            input.add(new Tuple2<>(value, key));
        }
        //Aplicamos sort en key
        JavaPairRDD<Integer, Long> rdd = sparkContext.parallelizePairs(input).sortByKey(true);
        JavaPairRDD<Integer, Long> sorted = rdd.sortByKey(false);

        return sorted;
    }

    //Print los 10 bigrams mas populares
    private static void printMostPopular(JavaPairRDD<Integer, Long> word, int title) {

        List<Tuple2<Integer, Long>> output = word.take(10);//Coge los 10 primeros de la lista

        if (title == 1) {
            System.out.println("\n*********** 10 most retweeted users ************\n");
        } else {
            System.out.println("\n*********** 10 most retweeted tweets ************\n");
        }
        for (Tuple2<?, ?> tuple : output) {
            if (title == 1) {
                System.out.println("Id User: " + tuple._2() + " - Number of retweets: " + tuple._1());
            } else {
                System.out.println("Id Tweet: " + tuple._2() + " - Number of retweets: " + tuple._1());
            }

        }
        System.out.println("\n*************************************************\n");

    }

}
