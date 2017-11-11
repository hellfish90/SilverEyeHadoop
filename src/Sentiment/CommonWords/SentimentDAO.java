package Sentiment.CommonWords;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.sun.javadoc.Doc;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by Marc on 7/6/16.
 */
public class SentimentDAO {

    MongoCollection collectionPositive, collectionNegative, collectionNeutral;

    public SentimentDAO(MongoDatabase db){

        this.collectionPositive =db.getCollection("PositiveWords");
        this.collectionNegative =db.getCollection("NegativeWords");
        this.collectionNeutral =db.getCollection("NeutralWords");
    }

    public void removeCollectionWords(){
        collectionPositive.drop();
        collectionNegative.drop();
        collectionNeutral.drop();
    }


    public void saveNeutralWord(String word, Integer ocurrences){

        Document BsonWord = new Document();
        BsonWord.put("_id",word);
        BsonWord.put("value", ocurrences);

        collectionNeutral.insertOne(BsonWord);

    }


    public List<String> getListWords(MongoCollection collection){

        FindIterable<Document> cursor = collection.find();

        List<String> list = new ArrayList<>();

        MongoCursor<Document> iterator = cursor.iterator();

        while (iterator.hasNext()){
            Document object = iterator.next();
            String word = (String) object.get("_id");
            list.add(word);
        }

        return list;
    }

    public void removeNeutralWords(){
        removeNeutralWords(collectionNegative);
        removeNeutralWords(collectionPositive);
    }

    private void removeNeutralWords(MongoCollection collection){

        for (String word:getNeutralWords()){
            Document document = new Document();
            document.put("_id",word);
            collection.deleteOne(document);

        }

    }


    public void updateCommonWords(){

        for (String word:getNeutralWords()){

            try{

                Document positiveWord = (Document) collectionPositive.find(new BasicDBObject("_id",word)).iterator().next();
                Document negativeWord = (Document) collectionNegative.find(new BasicDBObject("_id",word)).iterator().next();

                Integer positiveCount = (Integer) positiveWord.get("value");
                Integer negativeCount = (Integer) negativeWord.get("value");
                Integer result = positiveCount - negativeCount;

                Document searchQuery = new Document();
                searchQuery.put("_id",word);

                BasicDBObject newDocument = new BasicDBObject();


                if (result==0){//GOOD
                    newDocument.append("$set", new BasicDBObject().append("value", result));

                    collectionPositive.deleteOne(searchQuery);
                    collectionNegative.deleteOne(searchQuery);

                }else if(result>1){//GOOD
                    newDocument.append("$set", new BasicDBObject().append("value", result));
                    collectionNegative.deleteOne(searchQuery);
                    collectionPositive.updateOne(searchQuery,newDocument);
                }else{//GOOD
                    newDocument.append("$set", new BasicDBObject().append("value", result*-1));
                    collectionPositive.deleteOne(searchQuery);
                    collectionNegative.updateOne(searchQuery,newDocument);
                }

            }catch (NoSuchElementException e){
                System.out.println(word);
            }

        }

    }



    public List<String> getPositiveWords(){
        return getListWords(collectionPositive);
    }

    public List<String> getNegativeWords(){
        return getListWords(collectionNegative);
    }

    public List<String> getNeutralWords(){
        return getListWords(collectionNeutral);
    }

}
