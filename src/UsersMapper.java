import Models.Collections;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;

/**
 * Created by Marc on 30/6/16.
 */
public class UsersMapper extends Mapper<Object, BSONObject, LongWritable, Collections> {

    private final static IntWritable one = new IntWritable( 1 );
    private final Text word = new Text();

    public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

        Collections collections = new Collections();


        LongWritable userId = new LongWritable((Long) value.get("user"));

        //System.out.print(value.get("polarity").toString());

        BasicDBObject collectionsArray = (BasicDBObject)value.get("collections");

        if (collectionsArray!=null){
            if (((Integer)value.get("polarity"))==0){

                for ( String collection : collectionsArray.keySet() ){
                    collections.plusNeutralCollection(collection, (long) 1);
                }

            }else if (((Integer)value.get("polarity"))==1){

                for ( String collection : collectionsArray.keySet() ){
                    collections.plusPositiveCollection(collection, (long) 1);
                }

            }else{

                for ( String collection : collectionsArray.keySet() ){
                    collections.plusNegativeCollection(collection, (long) 1);
                }

            }

            for ( String collection : collectionsArray.keySet() ){
                collections.plusCounterCollection(collection, (long) 1);
            }

        }







        context.write( userId, collections );

    }
}