/**
 * Comando ejecucion de jar:
 * java -cp lab1-1.0-SNAPSHOT.jar upf.edu.TwitterFilter es output-es.txt myupfbucket C:\Users\mingo\Documents\NetBeansProjects\LSDS2020-master\lab1\Eurovision3.json
 */
package upf.edu;

import static com.fasterxml.jackson.databind.jsonFormatVisitors.JsonValueFormat.STYLE;
import upf.edu.filter.FileLanguageFilter;
import upf.edu.uploader.S3Uploader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TwitterFilter {

    /**
     * Calcula el tiempo entre dos valores(NANOSECONDS) y lo devuelve en formato
     * 00:00 min
     *
     * @param end fin
     * @param start inicio
     * @return tiempo en min.
     */
    public static String timeTotalSeconds(long end, long start) {

        long elapsedTime = end - start;
        double elapsedTimeInSecond = (double) elapsedTime / 1_000_000_000; // 1 second = 1_000_000_000 nano seconds      
        long convert = TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS); // TimeUnit        
        long min = (convert) / 60;
        long seg = convert - ((min * 60));
        String mn = Long.toString(min);
        String sc = Long.toString(seg);
        if (min < 10) {
            mn = "0" + mn;
        }
        if (seg < 10) {
            sc = "0" + sc;
        }
        return (mn + ":" + sc + " min");
    }

    public static void main(String[] args) throws IOException {

        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);
        int countTweets = 0;

        System.out.println("\nLanguage: " + language + ". Output file: " + outputFile + ". Destination bucket: " + bucket);

        long startTime = System.nanoTime();//Inicio tiempo filter tweets        
        for (String inputFile : argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);
            final FileLanguageFilter filter = new FileLanguageFilter(inputFile, outputFile, 0);
            filter.filterLanguage(language);

            countTweets += filter.getCountTweets();
        }
        long endTime = System.nanoTime(); //Fin tiempo

        System.out.println("\nNumber of tweets in language:'" + language + "' is " + countTweets);
        System.out.println("Time to filter tweets is " + timeTotalSeconds(endTime, startTime));

        startTime = System.nanoTime();//Inicio tiempo aws s3
        final S3Uploader uploader = new S3Uploader(bucket, "prefix", "upf");
        uploader.upload(Arrays.asList(outputFile));
        endTime = System.nanoTime(); //Fin tiempo

        System.out.println("Time to update the file " + outputFile + " in AWS S3 is " + timeTotalSeconds(endTime, startTime));

    }
}
