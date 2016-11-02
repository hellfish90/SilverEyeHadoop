/**
 * Created by Marc on 30/6/16.
 */
import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class WordCount {

    private static final Log log = LogFactory.getLog(WordCount.class);

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

        final Configuration conf = new Configuration();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:27017/Sentiment.NegativeTweet" );
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:2126/Sentiment.NegativeWords" );

        System.out.println( "Conf: " + conf );

        final Job job = new Job( conf, "word count" );

        job.setJarByClass( WordCount.class );

        job.setMapperClass( WordsMapper.class );

        //job.setCombinerClass( IntSumReducer.class );
        job.setReducerClass( IntSumReducer.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( IntWritable.class );

        job.setInputFormatClass( MongoInputFormat.class );
        job.setOutputFormatClass( MongoOutputFormat.class );

        System.exit( job.waitForCompletion( true ) ? 0 : 1 );
    }
}