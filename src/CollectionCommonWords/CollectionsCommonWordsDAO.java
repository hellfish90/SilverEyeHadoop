package CollectionCommonWords;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Marc on 7/6/16.
 */
public class CollectionsCommonWordsDAO {

    MongoCollection commonWordDb;

    public CollectionsCommonWordsDAO(MongoDatabase db, String collection){

        this.commonWordDb =db.getCollection("CommonWord"+collection);

    }

    public void saveCommonWords(String word, Integer ocurrences){

        Document BsonWord = new Document();
        BsonWord.put("_id",word);
        BsonWord.put("value", ocurrences);

        commonWordDb.insertOne(BsonWord);

    }


    public List<String> getListWords(MongoCollection collection, int limit){

        FindIterable<Document> cursor = collection.find().limit(limit);

        List<String> list = new ArrayList<String>();

        MongoCursor<Document> iterator = cursor.iterator();

        while (iterator.hasNext()){
            Document object = iterator.next();
            String word = (String) object.get("_id");
            list.add(word);
        }

        return list;
    }


    public List<String> getCommonWords(int limit){
        return getListWords(commonWordDb, limit);
    }

}