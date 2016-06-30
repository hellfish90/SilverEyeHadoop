package DAO;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.file.tfile.TFile;
import org.bson.Document;
import twitter4j.Status;

import java.util.Map;

/**
 * Created by Marc on 7/6/16.
 */
public class UserAnalyzedDAO {

    MongoCollection collection;
    MongoDatabase db;

    public UserAnalyzedDAO(MongoDatabase db){

        this.collection = db.getCollection("AnalyzedUser");
        this.db = db;
    }


    public void saveUser(Long userId, MapWritable positiveCollection, MapWritable negativeCollection,
                         MapWritable neutralCollection, MapWritable counterCollection){

        Document searchQuery = new Document().append("_id", userId);

        Document document = new Document();

        document.append("$set", new Document("_id", userId));
        collection.updateOne(searchQuery, document, (new UpdateOptions()).upsert(true));

        for (Map.Entry<Writable, Writable> entryCollection:positiveCollection.entrySet()){
            DBObject listItem = new BasicDBObject("positive."+entryCollection.getKey().toString(), ((LongWritable) entryCollection.getValue()).get());
            document.append("$set", listItem);
            collection.updateOne(searchQuery, document, (new UpdateOptions()).upsert(true));
        }

        for (Map.Entry<Writable, Writable> entryCollection:negativeCollection.entrySet()){
            DBObject listItem = new BasicDBObject("negative."+entryCollection.getKey().toString(), ((LongWritable) entryCollection.getValue()).get());
            document.append("$set", listItem);
            collection.updateOne(searchQuery, document, (new UpdateOptions()).upsert(true));
        }


        for (Map.Entry<Writable, Writable> entryCollection:neutralCollection.entrySet()){
            DBObject listItem = new BasicDBObject("neutral."+entryCollection.getKey().toString(), ((LongWritable) entryCollection.getValue()).get());
            document.append("$set", listItem);
            collection.updateOne(searchQuery, document, (new UpdateOptions()).upsert(true));
        }

        for (Map.Entry<Writable, Writable> entryCollection:counterCollection.entrySet()){
            DBObject listItem = new BasicDBObject("counter."+entryCollection.getKey().toString(), ((LongWritable) entryCollection.getValue()).get());
            document.append("$set", listItem);
            collection.updateOne(searchQuery, document, (new UpdateOptions()).upsert(true));
        }

    }

}
