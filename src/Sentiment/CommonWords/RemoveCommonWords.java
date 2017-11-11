package Sentiment.CommonWords;

import Sentiment.SentimentWordCount;

/**
 * Created by Marc on 13/9/17.
 */
public class RemoveCommonWords {

    public static void main( String[] args ) throws Exception{
        SentimentDAO sentimentDAO = new SentimentDAO(SentimentWordCount.getSentimentDatabase());

        sentimentDAO.removeNeutralWords();
    }

}
