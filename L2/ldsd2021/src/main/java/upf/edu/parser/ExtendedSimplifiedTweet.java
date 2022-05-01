/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package upf.edu.parser;

import java.io.Serializable;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.util.Optional;

public class ExtendedSimplifiedTweet implements Serializable {

    private final long tweetId; // the id of the tweet ('id')
    private final String text; // the content of the tweet ('text')
    private final long userId; // the user id ('user->id')
    private final String userName; // the user name ('user'->'name')
    private final long followersCount; // the number of followers ('user'->'followers_count')
    private final String language; // the language of a tweet ('lang')
    private final boolean isRetweeted; // is it a retweet? (the object 'retweeted_status' exists?)
    private final Long retweetedUserId; // [if retweeted] ('retweeted_status'->'user'->'id')
    private final Long retweetedTweetId; // [if retweeted] ('retweeted_status'->'id')
    private final long timestampMs; // seconds from epoch ('timestamp_ms')

    public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName, long followersCount, String language, boolean isRetweeted, Long retweetedUserId, Long retweetedTweetId, long timestampMs) {
        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.followersCount = followersCount;
        this.language = language;
        this.isRetweeted = isRetweeted;
        this.retweetedUserId = retweetedUserId;
        this.retweetedTweetId = retweetedTweetId;
        this.timestampMs = timestampMs;
    }

    public String getText() {
        return text;
    }

    public String getLanguage() {
        return language;
    }

    public boolean isIsRetweeted() {
        return isRetweeted;
    }

    public Long getRetweetedUserId() {
        return retweetedUserId;
    }

    public Long getRetweetedTweetId() {
        return retweetedTweetId;
    }
    

    /**
     * Returns a {@link ExtendedSimplifiedTweet} from a JSON String. If parsing
     * fails, for any reason, return an {@link Optional#empty()}
     *
     * @param jsonStr
     * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
     */
    public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {

        long tweetId = 0;
        String text = null;
        long userId = 0;
        long followersCount = 0;
        String userName = null;
        String language = null;
        long timestamp = 0;
        boolean isRetweeted = false;
        long retweetedUserId = 0;
        long retweetedTweetId = 0;
        boolean isValid = false;

        //Version gson 2.8.5
        Gson gson = new Gson();
        JsonElement element = gson.fromJson(jsonStr, JsonElement.class);
        JsonObject objectTweet = element.getAsJsonObject();

        //Verificamos que el tweet tiene las keys (id,text,user,lang,time)
        if (objectTweet.has("id") && objectTweet.has("text") && objectTweet.has("user") && objectTweet.has("lang") && objectTweet.has("timestamp_ms")) {
            //Verificamos que el key user tiene las keys (id,name)

            JsonObject userObject = objectTweet.get("user").getAsJsonObject();
            if (userObject.has("id") && userObject.has("name") && userObject.has("followers_count")) {
                //Anadimos valores del json a los atributos del objecto
                tweetId = objectTweet.get("id").getAsLong();
                text = objectTweet.get("text").getAsString();
                userId = userObject.get("id").getAsLong();
                followersCount = userObject.get("followers_count").getAsLong();
                userName = userObject.get("name").getAsString();
                language = objectTweet.get("lang").getAsString();
                timestamp = objectTweet.get("timestamp_ms").getAsLong();

                if (objectTweet.has("retweeted_status")) {

                    JsonObject retweetObject = objectTweet.get("retweeted_status").getAsJsonObject();
                    isRetweeted = true;
                    if (retweetObject.has("id") && retweetObject.has("user")) {
                        //Anadimos valores del json a los atributos del objecto
                        retweetedTweetId = retweetObject.get("id").getAsLong();
                        JsonObject userRetweetObject = retweetObject.get("user").getAsJsonObject();
                        if (userRetweetObject.has("id")) {
                            retweetedUserId = userRetweetObject.get("id").getAsLong();

                        }
                    }
                }
                isValid = true;
            }
        }

        Optional<ExtendedSimplifiedTweet> tweet = null;
        //Tweet no presente
        if (!isValid) {
            tweet = Optional.empty();
            return tweet;
        }

        // System.out.println("\n***************************\n" + tweetId + "\n" + text + "\n" + userId + "\n" + userName + "\n" + followersCount +  "\n" + language + "\n" + timestamp + "\n" +isRetweeted+ "\n" +retweetedUserId+ "\n" +retweetedTweetId+"\n***************************\n");
        ExtendedSimplifiedTweet newTweet = new ExtendedSimplifiedTweet(tweetId, text, userId, userName, followersCount, language, isRetweeted, retweetedUserId, retweetedTweetId, timestamp);
        tweet = Optional.of(newTweet);
        // System.out.println("\n***************************\n" + newTweet + "\n***************************\n"); //Print tweet
        return tweet;

    }

    @Override
    public String toString() {
        //Overriding the way how SimplifiedTweets are printed in console or the output file
        return new Gson().toJson(this); //Imprime en formato Json

    }
}
