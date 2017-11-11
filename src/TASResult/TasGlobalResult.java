package TASResult; /**
 * Created by Marc on 30/6/16.
 */

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.*;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class TasGlobalResult {

    private static final Log log = LogFactory.getLog(TasGlobalResult.class);

    private static String collection;

    private static final String R1="R1";
    private static final String R2="R2";

    private static final String OCC="OCC";

    private static final String SENTPOSOCC="SENTPOSOCC";
    private static final String SENTNEGOCC="SENTPOSOCC";

    private static final String SENTINCPOSOCC="SENTPOSOCC";
    private static final String SENTINCNEGOCC="SENTPOSOCC";


    public static class CommonWordsMapper extends Mapper<Object, BSONObject, Text, MapWritable> {

        public void map( Object key, BSONObject user, Context context ) throws IOException, InterruptedException{

            BSONObject TASResults = (BSONObject) user.get("TAS");

            boolean objectiveUser = false;

            Long tweetsNumber= 0l;
            Integer usersNumber= 0;

            HashMap<String,Long> collectionsPositiveSentimentOccurrences= null;
            HashMap<String,Long> collectionsNegativeSentimentOccurrences= null;
            HashMap<String,Long> collectionsPositiveSentimentPlus= null;
            HashMap<String,Long> collectionsNegativeSentimentPlus= null;

            HashMap<String, Long> collectionsOccurrences= null;

            HashMap<String, Long> collectionsResults1= null;
            HashMap<String,Long> collectionsResults2= null;

            if (TASResults!=null){



                BSONObject completeResults = (BSONObject) TASResults.get("completeResults");
                BSONObject sentiment = (BSONObject) TASResults.get("sentiment");
                collectionsOccurrences = (HashMap<String, Long>) TASResults.get("collectionsOccurrences");

                if (completeResults!=null && sentiment!=null && collectionsOccurrences!=null){

                    System.out.println("HAS____TAS");

                    collectionsResults1= (HashMap<String, Long>) completeResults.get("Result1");
                    collectionsResults2= (HashMap<String, Long>) completeResults.get("Result2");

                    collectionsPositiveSentimentOccurrences= (HashMap<String, Long>) sentiment.get("PositiveSentimentOccurrences");
                    collectionsNegativeSentimentOccurrences= (HashMap<String, Long>) sentiment.get("NegativeSentimentOccurrences");
                    collectionsPositiveSentimentPlus= (HashMap<String, Long>) sentiment.get("PositiveSentimentPlus");
                    collectionsNegativeSentimentPlus= (HashMap<String, Long>) sentiment.get("NegativeSentimentPlus");

                    tweetsNumber = (Long) user.get("tweetsNumber");
                    objectiveUser = true;

                }else{
                    objectiveUser = false;
                }


            }else{
                objectiveUser = false;
            }


            if (objectiveUser){
                MapWritable mapWritable = new MapWritable();
                for (Map.Entry<String,Long> entry : collectionsOccurrences.entrySet()) {
                    if(null != entry.getKey() && null != entry.getValue()){
                        mapWritable.put(new Text(entry.getKey()),new Text(entry.getValue().toString()));
                    }
                }
                context.write( new Text("PositiveSentimentOccurrences"), mapWritable);
            }

        }
    }


    public static class IntSumReducer extends Reducer<Text, MapWritable, Object, BSONObject> {

        public void reduce( Text key, Iterable<MapWritable> values, Context context )
                throws IOException, InterruptedException{

            HashMap<String,Long> collectionValues = new HashMap<>();

            for (MapWritable collections: values){
                for (Map.Entry collection: collections.entrySet() ){
                    String collectionName = ((Text)collection.getKey()).toString();
                    Long collectionValue = Long.valueOf(((Text)collection.getValue()).toString());

                    if (!collectionValues.keySet().contains(collectionName)){
                        collectionValues.put(collectionName,collectionValue);
                    }else{
                        collectionValues.put(collectionName,collectionValue +collectionValues.get(collectionName));
                    }
                }
            }



            BSONObject document = new BasicBSONObject();

            for (Map.Entry<String, Long> collection:collectionValues.entrySet()){
                document.put(key.toString(), new BasicBSONObject(collection.getKey(),collection.getValue()));
                //collection.updateOne(searchQuery, document, (new UpdateOptions()).upsert(true));
            }

            context.write("TASResult", document);

        }
    }

    public static void main( String[] args ) throws Exception{



        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:27017/DummyDB.DummyTwitterUsers" );
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:27017/DummyDB.GlobalResults" );

        System.out.println( "Conf: " + conf );

        final Job job = new Job( conf, "word count" );

        job.setJarByClass( TasGlobalResult.class );

        job.setMapperClass( CommonWordsMapper.class );

        //job.setCombinerClass( IntSumReducerCommonWords.class );
        job.setReducerClass( IntSumReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( MapWritable.class );

        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        System.exit( job.waitForCompletion( true ) ? 0 : 1 );



    }
}