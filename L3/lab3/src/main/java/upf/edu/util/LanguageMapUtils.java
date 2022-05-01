package upf.edu.util;

import java.util.Arrays;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

public class LanguageMapUtils {

    public static JavaPairRDD<String, String> buildLanguageMap(JavaRDD<String> lines) {

        //Guardamos JavaPairRDD <(language code = en) , (full language name = English)> 
        JavaPairRDD<String, String> separLine = lines            
                .mapToPair(s -> new Tuple2<>(s.split("\\t")[1],s.split("\\t")[2]))
                .filter(s-> !s._1.isEmpty())
                .distinct();

        return separLine;
    }
}
