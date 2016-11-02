
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.*;

/**
 * Created by Marc on 30/6/16.
 */
public class WordsMapper extends Mapper<Object, BSONObject, Text, IntWritable> {

    String[] hashtagsHappy = {":)", ";-)", "=)", ":D","ヽ(•‿•)ノ","\uD83D\uDE04", "\uD83D\uDE0D",
            "\uD83D\uDE18","❤","☺","\uD83D\uDE04","\uD83D\uDE06","\uD83D\uDE02","\uD83D\uDE01","\uD83D\uDE0B"};

    String[] hashtagsSad = {":(", ":-(", "=(", ";(", "\uD83D\uDE14","\uD83D\uDE1E","\uD83D\uDE2D","\uD83D\uDE2B",
            "\uD83D\uDE21", "\uD83D\uDE20", "\uD83D\uDE24", "\uD83D\uDCA5", "\uD83D\uDCA2"};




    public void map( Object key, BSONObject value, Context context ) throws IOException, InterruptedException{

        ArrayList<String> finalWords= new ArrayList<>();

        ArrayList<String> hashtagsHappyList = new ArrayList<>(Arrays.asList(hashtagsHappy));
        ArrayList<String> hashtagsSadList = new ArrayList<>(Arrays.asList(hashtagsSad));

        String text = (String) value.get("text");

        List<String> wordslist =  new ArrayList<>(Arrays.asList(text.toLowerCase().split(" ")));

        ArrayList<String> pairWords = new ArrayList<>();


        //Make pairs
        Iterator<String> iterator = wordslist.iterator();

        String lastWord = iterator.next();

        while(iterator.hasNext()){
            String nextWord =  iterator.next();
            pairWords.add(lastWord+" "+nextWord);
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
                     word.length()>3 &&
                    !hashtagsSadList.contains(word)){
                finalWords.add(word);
            }
        }

        for(String words: finalWords){
            context.write( new Text(words), new IntWritable(1));
        }




    }
}