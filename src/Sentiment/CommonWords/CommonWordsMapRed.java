package Sentiment.CommonWords; /**
 * Created by Marc on 30/6/16.
 */
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;


import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class CommonWordsMapRed {

    public static String BAD_WORDS = "BAD_WORDS";
    public static boolean dev=true;
    static MongoDatabase db;


    private static final Log log = LogFactory.getLog(CommonWordsMapRed.class);


    public static class WordsMapperCommonWords extends Mapper<Object, BSONObject, Text, IntWritable> {



        public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

            ArrayList<String> finalWords= new ArrayList<>();

            String word = (String) value.get("_id");

            Configuration configuration = context.getConfiguration();

            ArrayList<String> badWords =
                    new ArrayList<String>(Arrays.asList(configuration.getStrings(CommonWordsMapRed.BAD_WORDS)));


            if(badWords.contains(word)){
                context.write( new Text(word), new IntWritable(1));
            }
        }


    }


    public static class IntSumReducerCommonWords extends Reducer<Text, IntWritable, Object, BSONObject> {

        public void reduce( Text key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException{

                Integer totalValue = 0;

                for (IntWritable value: values){
                    totalValue += value.get();
                }

                context.write(key, new BasicBSONObject("value",totalValue));
        }
    }

}