/**
 * Created by Marc on 30/6/16.
 */
import java.io.*;

import DAO.UserAnalyzedDAO;
import Models.Collections;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class WordCount {

    private static final Log log = LogFactory.getLog(WordCount.class);

    public static class IntSumReducer extends Reducer<LongWritable, Collections, LongWritable, Collections> {

        private final IntWritable result = new IntWritable();
        private UserAnalyzedDAO userAnalyzedDAO;

        protected void setup(Context context) throws IOException, InterruptedException {
            MongoClient mongo = new MongoClient("localhost", 27017);
            MongoDatabase db = mongo.getDatabase("SilverEye");
            userAnalyzedDAO = new UserAnalyzedDAO(db);


        }

        public void reduce( LongWritable key, Iterable<Collections> values, Context context )
                throws IOException, InterruptedException{



            Collections collections = new Collections();

            for (Collections collection: values){

                //Plus positive collections
                for (Writable positiveCollection: collection.getPositiveCollections().keySet()){

                    String collectionKey = positiveCollection.toString();
                    Long value = ((LongWritable)collection.getPositiveCollections().get(positiveCollection)).get();
                    collections.plusPositiveCollection(collectionKey,value);

                }

                //Plus negative collections
                for (Writable negativeCollection: collection.getNegativeCollections().keySet()){

                    String collectionKey = negativeCollection.toString();
                    Long value = ((LongWritable)collection.getNegativeCollections().get(negativeCollection)).get();
                    collections.plusNegativeCollection(collectionKey,value);

                }

                //Plus neutral collections
                for (Writable neutralCollection: collection.getNeutralCollections().keySet()){

                    String collectionKey = neutralCollection.toString();
                    Long value = ((LongWritable)collection.getNeutralCollections().get(neutralCollection)).get();
                    collections.plusNeutralCollection(collectionKey,value);

                }

                //Plus counter collections
                for (Writable counterCollection: collection.getCounterCollection().keySet()){

                    String collectionKey = counterCollection.toString();
                    Long value = ((LongWritable)collection.getCounterCollection().get(counterCollection)).get();
                    collections.plusCounterCollection(collectionKey,value);

                }

            }

            userAnalyzedDAO.saveUser(key.get(),collections.getPositiveCollections(),
                    collections.getNegativeCollections(), collections.getNeutralCollections(),
                    collections.getCounterCollection());
        }
    }

    public static void main( String[] args ) throws Exception{

        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost/SilverEye.AnalyzedTweet" );
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost/SilverEye.AnalyzedUser" );

        System.out.println( "Conf: " + conf );

        final Job job = new Job( conf, "word count" );

        job.setJarByClass( WordCount.class );

        job.setMapperClass( UsersMapper.class );

        //job.setCombinerClass( IntSumReducer.class );
        job.setReducerClass( IntSumReducer.class );

        job.setOutputKeyClass( LongWritable.class );
        job.setOutputValueClass( Collections.class );

        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    }
}