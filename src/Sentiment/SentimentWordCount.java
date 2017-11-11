package Sentiment; /**
 * Created by Marc on 30/6/16.
 */
import java.io.*;
import java.util.List;

import Sentiment.CommonWords.CommonWordsMapRed;
import Sentiment.CommonWords.SentimentDAO;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;
import org.apache.htrace.commons.logging.Log;
import org.apache.htrace.commons.logging.LogFactory;

/**
 * test.in db.in.insert( { x : "eliot was here" } ) db.in.insert( { x : "eliot is here" } ) db.in.insert( { x : "who is
 * here" } ) =
 */
public class SentimentWordCount {

    public static String BAD_WORDS = "BAD_WORDS";
    public static boolean dev=true;
    static MongoDatabase db;

    private static final Log log = LogFactory.getLog(SentimentWordCount.class);

    private static Job executeNegativeWordCounter() throws IOException {
        final Configuration confNegative = new Configuration();
        MongoConfigUtil.setInputURI( confNegative, "mongodb://localhost:27017/Sentiment.NegativeTweet" );
        MongoConfigUtil.setOutputURI( confNegative, "mongodb://localhost:27017/Sentiment.NegativeWords" );

        System.out.println( "Conf: " + confNegative );

        final Job jobNegative = new Job( confNegative, "Negative Count" );

        jobNegative.setJarByClass( SentimentWordCount.class );

        jobNegative.setMapperClass( SentimentWordsMapRed.SentimentWordsMapper.class );

        //jobPositive.setCombinerClass( IntSumReducerCommonWords.class );
        jobNegative.setReducerClass( SentimentWordsMapRed.IntSumReducerSentimentWords.class );

        jobNegative.setOutputKeyClass( Text.class );
        jobNegative.setOutputValueClass( IntWritable.class );

        jobNegative.setInputFormatClass( MongoInputFormat.class );
        jobNegative.setOutputFormatClass( MongoOutputFormat.class );

        return jobNegative;

    }

    private static Job executeCommonWordsCounter() throws IOException{
        JobConf conf = new JobConf();
        MongoConfigUtil.setInputURI( conf, "mongodb://localhost:27017/Sentiment.PositiveWords" );
        MongoConfigUtil.setOutputURI( conf, "mongodb://localhost:27017/Sentiment.NeutralWords" );

        SentimentDAO sentimentDAO = new SentimentDAO(getSentimentDatabase());

        List<String> listbadWord = sentimentDAO.getNegativeWords();

        String[] badWordArray = listbadWord.toArray(new String[listbadWord.size()]);
        conf.setStrings(BAD_WORDS, badWordArray);

        final Job jobCommonWords = Job.getInstance(conf, "GoodBadWordsTrendsRefined" );

        jobCommonWords.setJarByClass( SentimentWordCount.class );

        jobCommonWords.setMapperClass( CommonWordsMapRed.WordsMapperCommonWords.class );

        //jobCommonWords.setCombinerClass( IntSumReducerCommonWords.class );
        jobCommonWords.setReducerClass( CommonWordsMapRed.IntSumReducerCommonWords.class );

        jobCommonWords.setOutputKeyClass( Text.class );
        jobCommonWords.setOutputValueClass( IntWritable.class );

        jobCommonWords.setInputFormatClass( MongoInputFormat.class );
        jobCommonWords.setOutputFormatClass( MongoOutputFormat.class );

        return jobCommonWords;
    }

    private static Job executePositiveWordCounter() throws IOException {

        final Configuration confPositive = new Configuration();
        MongoConfigUtil.setInputURI( confPositive, "mongodb://localhost:27017/Sentiment.PositiveTweet" );
        MongoConfigUtil.setOutputURI( confPositive, "mongodb://localhost:27017/Sentiment.PositiveWords" );

        System.out.println( "Conf: " + confPositive );

        final Job jobPositive = new Job( confPositive, "Positive Count" );

        jobPositive.setJarByClass( SentimentWordCount.class );

        jobPositive.setMapperClass( SentimentWordsMapRed.SentimentWordsMapper.class );

        //jobPositive.setCombinerClass( IntSumReducerCommonWords.class );
        jobPositive.setReducerClass( SentimentWordsMapRed.IntSumReducerSentimentWords.class );

        jobPositive.setOutputKeyClass( Text.class );
        jobPositive.setOutputValueClass( IntWritable.class );

        jobPositive.setInputFormatClass( MongoInputFormat.class );
        jobPositive.setOutputFormatClass( MongoOutputFormat.class );

        return jobPositive;

    }

    public static void main( String[] args ) throws Exception{

        SentimentDAO sentimentDAO = new SentimentDAO(SentimentWordCount.getSentimentDatabase());
        sentimentDAO.removeCollectionWords();


        final Job jobPositive = executePositiveWordCounter();
        final Job jobNegative = executeNegativeWordCounter();
        final Job jobCommonWords;



        if (jobPositive.waitForCompletion( true )){
            System.out.println("\n\n\n Positive Count Job Finish\n\n\n");

            if (jobNegative.waitForCompletion( true )){
                System.out.println("\n\n\n Negative Count Job Finish\n\n\n");

                jobCommonWords = executeCommonWordsCounter();

                if(jobCommonWords.waitForCompletion( true )){
                    System.out.println("\n\n\n Common Words Count Job Finish\n\n\n");
                }else{
                    System.exit(-1);
                }
            }else{
                System.exit(-1);
            }

        }else{
            System.exit(-1);
        }


    }

    public static MongoDatabase getSentimentDatabase(){

        if(dev){

            MongoClient mongo = new MongoClient("0.0.0.0", 27017);
            db = mongo.getDatabase("Sentiment");

        }else{
            MongoClient mongo = new MongoClient("192.168.101.85", 27017);
            db = mongo.getDatabase("Sentiment");
        }



        return db;
    }
}