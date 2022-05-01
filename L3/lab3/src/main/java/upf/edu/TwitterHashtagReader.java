/**
 * cd C:\Users\mingo\Documents\NetBeansProjects\lab3\target
 * spark-submit --master local[2] --class upf.edu.TwitterHashtagReader lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties es
 * spark-submit --master local[2] --class upf.edu.TwitterHashtagReader --conf spark.driver.extraJavaOptions=-Dlog4j.configuration=file:C:/Users/mingo/Documents/NetBeansProjects/lab3/src/main/resources/log4j.properties lab3-1.0-SNAPSHOT.jar ..\src\main\resources\application.properties es
 */
package upf.edu;

import twitter4j.auth.OAuthAuthorization;
import upf.edu.model.HashTagCount;
import upf.edu.storage.DynamoHashTagRepository;
import upf.edu.util.ConfigUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class TwitterHashtagReader {

    public static void main(String[] args) throws InterruptedException, IOException {
        String propertiesFile = args[0];
        String language = args[1];
        OAuthAuthorization auth = ConfigUtils.getAuthorizationFromFileProperties(propertiesFile);

        DynamoHashTagRepository dynamoDB = new DynamoHashTagRepository();
        List<HashTagCount> answer = dynamoDB.readTop10(language);
        
        System.out.println("\n****** Top 10 Hasthags*******\n");
        for (Iterator<HashTagCount> iterator = answer.iterator(); iterator.hasNext();) {
            HashTagCount h = iterator.next();
            System.out.println(h.toString());

        }
    }
}
