package Sentiment;

import Sentiment.CommonWords.SentimentDAO;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Marc on 14/9/17.
 */
public class SentimentWordsMapRed {


    public static class SentimentWordsMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

        String[] hashtagsHappy = {":)", ";-)", "=)", ":D","ヽ(•‿•)ノ","\uD83D\uDE04", "\uD83D\uDE0D",
                "\uD83D\uDE18","❤","☺","\uD83D\uDE04","\uD83D\uDE06","\uD83D\uDE02","\uD83D\uDE01","\uD83D\uDE0B"};

        String[] hashtagsSad = {":(", ":-(", "=(", ";(", "\uD83D\uDE14","\uD83D\uDE1E","\uD83D\uDE2D","\uD83D\uDE2B",
                "\uD83D\uDE21", "\uD83D\uDE20", "\uD83D\uDE24", "\uD83D\uDCA5", "\uD83D\uDCA2"};

        private String removeComas(String text) {
            return text.replace(","," ");
        }


        public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

            ArrayList<String> finalWords= new ArrayList<>();

            //ArrayList<String> hashtagsHappyList = new ArrayList<>(Arrays.asList(hashtagsHappy));
            //ArrayList<String> hashtagsSadList = new ArrayList<>(Arrays.asList(hashtagsSad));

            String text = (String) value.get("text");
            text = removeUrl(text);
            text = removeComas(text);
            text = removeBreakLine(text);
            text = removeStrangeCharacters(text);
            text = removeBigSpaces(text);


            List<String> wordslist =  new ArrayList<>(Arrays.asList(text.toLowerCase().split(" ")));

            ArrayList<String> pairWords = new ArrayList<>();


            //Make pairs
            Iterator<String> iterator = wordslist.iterator();

            String lastWord = iterator.next();


            //TODO Fer que salti les paraules menors de 3 caracters i no les emparelli
            while(iterator.hasNext()){
                String nextWord =  iterator.next();
                if (lastWord.length()>3 && nextWord!= " "){
                    pairWords.add(lastWord+" "+nextWord);
                }

                lastWord = nextWord;
            }

            for (String words:pairWords){
                wordslist.add(words);
            }


            //http://slides.com/mertkahyaoglu/twitter-sentiment-analysis-4#/4
            for (String word: wordslist){
                if (!word.contains("#") &&
                        !word.contains("@") &&
                        !word.contains("http://") &&
                        !word.contains("https://") &&
                        !word.contains("RT")&&
                        word.length()>3){
                    finalWords.add(word);
                }
            }

            for(String words: finalWords){
                context.write( new Text(words), new IntWritable(1));
            }


        }

        private String removeBigSpaces(String text) { return text.replace("  "," ").replace("   "," ").replace("    "," ");
        }

        private String removeBreakLine(String text) {
            return text.replace("\n"," ");
        }

        private String removeStrangeCharacters(String text) {
            return text.replaceAll("[_+-.,@#$%&*\\/|<>\"']|[0-9]"," ");
        }

        private String removeUrl(String commentstr)
        {
            try{
                String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
                Pattern p = Pattern.compile(urlPattern,Pattern.CASE_INSENSITIVE);
                Matcher m = p.matcher(commentstr);
                int i = 0;
                while (m.find()) {
                    commentstr = commentstr.replaceAll(m.group(i),"").trim();
                    i++;
                }
                return commentstr;

            }catch (Exception e){
                return commentstr;
            }


        }

    }





    public static class IntSumReducerSentimentWords extends Reducer<Text, IntWritable, Object, BSONObject> {

        public void reduce( Text key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException{

            Integer totalValue = 0;

            for (IntWritable value: values){
                totalValue += value.get();
            }

            if (totalValue>10){
                context.write(key, new BasicBSONObject("value",totalValue));
            }


        }
    }



    public static void main( String[] args ) throws Exception{

    }

}
