package upf.edu.storage;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;
import twitter4j.Status;
import upf.edu.model.HashTagCount;
import java.io.Serializable;
import java.util.*;
import twitter4j.HashtagEntity;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;


public class DynamoHashTagRepository implements IHashtagRepository, Serializable {

    @Override
    public void write(Status tweet) {

        String tableName = "LSDS2020-TwitterHashtags";
        Table table = new DynamoDB(client()).getTable(tableName);

        try {
            HashtagEntity hashtags[] = tweet.getHashtagEntities();
            for (HashtagEntity h : hashtags) {

                //Si existe hashtag en BD, actualizamos
                if (existItem(h, tweet, table)) {
                    update(h, tweet, table);
                } else {
                    insert(h, tweet, table);//Nuevo item
                }
            }
        } catch (Exception e) {
            System.err.println("Unable to update item: ");
            System.err.println(e.getMessage());
        }
    }

    @Override
    public List<HashTagCount> readTop10(String lang) {

        String tableName = "LSDS2020-TwitterHashtags";

        // Scan items for language with a year attribute equals lang
        HashMap<String, Condition> scanFilter = new HashMap<String, Condition>();
        Condition condition = new Condition().withComparisonOperator(ComparisonOperator.EQ)
                .withAttributeValueList(new AttributeValue().withS(lang));
        scanFilter.put("language", condition);
        // Consulta con filtro
        ScanRequest scanRequest = new ScanRequest(tableName).withScanFilter(scanFilter);
        ScanResult scanResult = client().scan(scanRequest);

        List<Map<String, AttributeValue>> items = scanResult.getItems();
        List<HashTagCount> hashtags = new ArrayList<>();

        for (Map<String, AttributeValue> pair : items) {

            String hashTag = pair.get("hashtag").getS();
            String language = pair.get("language").getS();
            Long count = Long.parseLong(pair.get("tweetCount").getN());

            HashTagCount htc = new HashTagCount(hashTag, language, count);
            hashtags.add(htc);
        }

        //Ordena ascendente
        hashtags.sort(Comparator.comparing(HashTagCount::getCount));
        Collections.reverse(hashtags); //Descendente

        //Top 10 tweetCount
        List<HashTagCount> top10 = new ArrayList<HashTagCount>(hashtags.subList(0, hashtags.size()-(hashtags.size() - 10)));
        return top10;
    }

    public AmazonDynamoDB client() {

        final AmazonDynamoDB client = AmazonDynamoDBClientBuilder
                .standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new ProfileCredentialsProvider("upf"))
                .build();
        return client;

    }

    public void createTable() {
        String tableName = "LSDS2020-TwitterHashtags";

        DynamoDB dynamoDB = new DynamoDB(client());

        try {
            System.out.println("Attempting to create table; please wait...");
            Table table = dynamoDB.createTable(tableName,
                    Arrays.asList(new KeySchemaElement("hashtag", KeyType.HASH), // Partition key
                            new KeySchemaElement("language", KeyType.RANGE)), // Sort key
                    Arrays.asList(new AttributeDefinition("hashtag", ScalarAttributeType.S),
                            new AttributeDefinition("language", ScalarAttributeType.S)),
                    new ProvisionedThroughput(10L, 10L));

            table.waitForActive();
            System.out.println("Success.  Table status: " + table.getDescription().getTableStatus());

        } catch (Exception e) {
            System.err.println("Unable to create table: ");
            System.out.println("Table already exists");
        }

    }

    public void update(HashtagEntity h, Status tweet, Table table) {

        //Actualizamos tweetCount
        UpdateItemSpec updateTweetCount = new UpdateItemSpec().withPrimaryKey("hashtag", h.getText(), "language", tweet.getLang())
                .withUpdateExpression("set tweetCount = tweetCount + :r")
                .withValueMap(new ValueMap().withNumber(":r", 1))
                .withReturnValues(ReturnValue.UPDATED_NEW);
        //Actualizamos lista tweetId
        UpdateItemSpec updateListTweetId = new UpdateItemSpec().withPrimaryKey("hashtag", h.getText(), "language", tweet.getLang())
                .withUpdateExpression("set tweetId = list_append(:prepend_value, tweetId)")
                .withValueMap(new ValueMap().withList(":prepend_value", tweet.getId()))
                .withReturnValues(ReturnValue.UPDATED_NEW);

        UpdateItemOutcome outTweetCount = table.updateItem(updateTweetCount);
        UpdateItemOutcome outTweetId = table.updateItem(updateListTweetId);
        System.out.println("Update Item succeeded:\n" + outTweetCount.getItem().toJSONPretty() + "\n" + outTweetId.getItem().toJSONPretty());

    }

    public void insert(HashtagEntity h, Status tweet, Table table) {
        //Nuevo item
        Item newItem = new Item().withPrimaryKey("hashtag", h.getText(), "language", tweet.getLang())
                .withLong("tweetCount", 1)
                .withList("tweetId", tweet.getId());
        table.putItem(newItem);
        System.out.println("Insert Item: " + "{Hashtag: " + h.getText() + " , Language: " + tweet.getLang() + "}");

    }

    public boolean existItem(HashtagEntity h, Status tweet, Table table) {

        GetItemSpec getItem = new GetItemSpec().withPrimaryKey("hashtag", h.getText(), "language", tweet.getLang());
        if (table.getItem(getItem) == null) {//No existe
            return false;
        }
        return true;
    }

}
