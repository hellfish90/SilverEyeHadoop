package CollectionCommonWords; /**
 * Created by Marc on 30/6/16.
 */

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class CollectionCommonWordCount {

    private static final Log log = LogFactory.getLog(CollectionCommonWordCount.class);

    private static String collection;


    public static class CommonWordsMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

        public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

            BSONObject collections = (BSONObject) value.get("collections");
            HashMap<String,Long> totalCollection = null;
            HashMap<String,Long> tweetCollection = null;

            boolean objectiveTweet = false;

            if (collections!=null){
                totalCollection = (HashMap<String, Long>) collections.get("total");
                tweetCollection = (HashMap<String, Long>) collections.get("tweet");
            }


            if (totalCollection!=null && totalCollection.keySet().contains(collection)){
                objectiveTweet=true;
            }
            if (tweetCollection!=null && tweetCollection.keySet().contains(collection)) {
                objectiveTweet = true;
            }

            if (objectiveTweet){
                String text = (String) value.get("text");

                if (text!=null){
                    List<String> wordslist =  new ArrayList<>(Arrays.asList(text.toLowerCase().split(" ")));

                    for(String word: wordslist){

                        if (word.length()>1){
                            context.write( new Text(word), new IntWritable(1));
                        }

                    }
                }


            }

        }
    }


    public static class IntSumReducer extends Reducer<Text, IntWritable, Object, BSONObject> {

        public void reduce( Text key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException{

            Integer totalValue = 0;

            for (IntWritable value: values){
                totalValue += value.get();
            }

            context.write(key, new BasicBSONObject("value",totalValue));

        }
    }

    public static void main( String[] args ) throws Exception{

        if (args[0]!=null){
            collection = args[0];
            System.out.println(args[0]);
        }

        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:27017/DummyDB.TweetsAnalyzed" );
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:27017/DummyDB.Sentiment.CommonWordsMapRed"+collection );

        System.out.println( "Conf: " + conf );

        final Job job = new Job( conf, "word count" );

        job.setJarByClass( CollectionCommonWordCount.class );

        job.setMapperClass( CommonWordsMapper.class );

        //job.setCombinerClass( IntSumReducerCommonWords.class );
        job.setReducerClass( IntSumReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        System.exit( job.waitForCompletion( true ) ? 0 : 1 );



    }
}