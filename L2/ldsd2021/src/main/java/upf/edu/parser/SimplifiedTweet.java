package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.IOException;

import java.util.Optional;

public class SimplifiedTweet {

    static int cont = 0;

    private final long tweetId;			  // the id of the tweet ('id')
    private final String text;  		  // the content of the tweet ('text')
    private final long userId;			  // the user id ('user->id')
    private final String userName;		  // the user name ('user'->'name')
    private final String language;                // the language of a tweet ('lang')
    private final long timestampMs;		  // seconds from epoch ('timestamp_ms')

    public SimplifiedTweet(long tweetId, String text, long userId, String userName, String language, long timestampMs) {
        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.language = language;
        this.timestampMs = timestampMs;
    }

    public String getLanguage() {
        return language;
    }

    /**
     * Returns a {@link SimplifiedTweet} from a JSON String. If parsing fails,
     * for any reason, return an {@link Optional#empty()}
     *
     * @param jsonStr
     * @return an {@link Optional} of a {@link SimplifiedTweet}
     */
    public static Optional<SimplifiedTweet> fromJson(String jsonStr) {

        long tweetId = 0;
        String text = null;
        long userId = 0;
        String userName = null;
        String language = null;
        long timestamp = 0;
        boolean isValid = false;

        //Version gson 2.8.5
        Gson gson = new Gson();
        JsonElement element = gson.fromJson(jsonStr, JsonElement.class);
        JsonObject objectTweet = element.getAsJsonObject();

        //Verificamos que el tweet tiene las keys (id,text,user,lang,time)
        if (objectTweet.has("id") && objectTweet.has("text") && objectTweet.has("user") && objectTweet.has("lang") && objectTweet.has("timestamp_ms")) {
            //Verificamos que el key user tiene las keys (id,name)
            JsonObject userObject = objectTweet.get("user").getAsJsonObject();
            if (userObject.has("id") && userObject.has("name")) {
                //Anadimos valores del json a los atributos del objecto
                tweetId = objectTweet.get("id").getAsLong();
                text = objectTweet.get("text").getAsString();
                userId = userObject.get("id").getAsLong();
                userName = userObject.get("name").getAsString();
                language = objectTweet.get("lang").getAsString();
                timestamp = objectTweet.get("timestamp_ms").getAsLong();
                isValid = true;
            }
        }

        Optional<SimplifiedTweet> tweet = null;
        //Tweet no presente
        if (!isValid) {
            tweet = Optional.empty();
            return tweet;
        }

        // System.out.println("\n" + tweetId + "\n" + text + "\n" + userId + "\n" + userName + "\n" + language + "\n" + timestamp + "\n");
        SimplifiedTweet newTweet = new SimplifiedTweet(tweetId, text, userId, userName, language, timestamp);
        tweet = Optional.of(newTweet);
        // System.out.println(newTweet); //Print tweet

        return tweet;

    }

    @Override
    public String toString() {
        //Overriding the way how SimplifiedTweets are printed in console or the output file
        return new Gson().toJson(this); //Imprime en formato Json

    }

}
