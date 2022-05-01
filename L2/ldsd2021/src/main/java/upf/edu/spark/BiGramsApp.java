/**
 * Comando ejecucion:
 * spark-submit --master local[4] --class upf.edu.spark.BiGramsApp lab2-1.0-SNAPSHOT.jar es ./output-folder C:\Eurovision
 */
package upf.edu.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import upf.edu.parser.ExtendedSimplifiedTweet;

public class BiGramsApp {

    public static void main(String[] args) {
        String language = args[0];
        String outputFile = args[1];
        String inputFile = args[2];

        System.out.println("\nLanguage: " + language + "\nOutput file: " + outputFile + "\nInput file: " + inputFile + "\n");

        SparkConf conf = new SparkConf().setAppName("Filter Tweets").setMaster("local");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        JavaRDD<String> lines = sparkContext.textFile(inputFile);
        JavaPairRDD<String, Integer> biGrams = lines
                .filter(line -> !line.isEmpty()) //Primero filtramos todos los strings que estas vacios (problems NullPointer)
                .map(line -> ExtendedSimplifiedTweet.fromJson(line)) //JavaRDD cada fila contiene un Optional<SimpleTweet>
                .filter(tweet -> tweet.isPresent()) //Filtramos por los tweet existentes. Descartamos los null
                .map(tweet -> tweet.get()) //Mapeamos de Optional<SimpleTweet> a SimpleTweet
                .filter(tweet -> tweet.getLanguage().equals(language)) //Filtamos por lenguaje
                .filter(tweet -> !tweet.isIsRetweeted())//filtramos por tweets sin retuitear             
                .map(tweet -> biGram(tweet)) //return String con bi-grams
                .flatMap(text -> Arrays.asList(text.split("[ ]")).iterator())
                .map(word -> normalise(word))//Eliminamos espacios
                .mapToPair(word -> new Tuple2<>(word, 1)) 
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<Integer, String> biGramsOrder = orderByValue(biGrams, sparkContext); //Ordena por valor
        printMostPopular(biGramsOrder);//Printa los 10 bigrams mas populares
        System.out.println("\n***************************\nNumber of bi-grams: " + biGrams.count()+"\n***************************\n");
        biGrams.saveAsTextFile(outputFile);

    }
    //Ordena JavaPairRDD por valor
    private static JavaPairRDD<Integer, String> orderByValue(JavaPairRDD<String, Integer> biGrams, JavaSparkContext sparkContext) {

        List<Tuple2<Integer, String>> input = new ArrayList<>();
        //Intercambiamos key, value
        for (Tuple2<String, Integer> tuple : biGrams.collect()) {
            int value = (Integer) tuple._2();
            String key = (String) tuple._1();
            input.add(new Tuple2<>(value, key));
        }
        //Aplicamos sort en key
        JavaPairRDD<Integer, String> rdd = sparkContext.parallelizePairs(input).sortByKey(true);
        JavaPairRDD<Integer, String> sorted = rdd.sortByKey(false);

        return sorted;
    }
    //Print los 10 bigrams mas populares
    private static void printMostPopular(JavaPairRDD<Integer, String> biGrams) {

        List<Tuple2<Integer, String>> output = biGrams.take(10);//Coge los 10 primeros de la lista
        System.out.println("\n*********** 10 bi-grams most popular ************\n");
        for (Tuple2<?, ?> tuple : output) {           
            System.out.println(tuple._2()+": "+tuple._1());         
        }
        System.out.println("\n*************************************************\n");
    }

    //Elimina espacios
    private static String normalise(String word) {
        return word.trim();
    }

    //Devuelve un string con bi-grams separados por un espacio
    private static String biGram(ExtendedSimplifiedTweet tweet) {

        String[] arr = tweet.getText().split(" ");
        String conc = "";
        for (int i = 0; i < arr.length - 1; i++) {
            arr[i] = arr[i] + arr[i + 1];
            if(!arr[i].equals("")){
            conc += arr[i] + " ";
            }
        }
        return conc;
    }
}
