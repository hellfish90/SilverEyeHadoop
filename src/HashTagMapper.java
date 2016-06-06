import Models.HashTagPair;
import Models.HashTagPolarity;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by Marc on 1/6/16.
 */
public class HashTagMapper extends Mapper<Object, Text, HashTagPair, HashTagPolarity> {



    private Set<String> negativeWords = new HashSet();
    private Set<String> positiveWords = new HashSet();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try{
            URI[]  stopWordsFiles = context.getCacheFiles();
            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                for(URI stopWordFile : stopWordsFiles) {
                    readFile(stopWordFile.getPath().substring(stopWordFile.getPath().lastIndexOf('/') + 1));
                }
            }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        JSONObject jsonObj;
        String text, lang;
        Double positive, negative;
        List<String> hashtags;
        HashTagPolarity hashTagPolarity = new HashTagPolarity();

        try {
            jsonObj = new JSONObject(value.toString());

            lang = (String) jsonObj.get("lang");
            text = (String) jsonObj.get("text");

            if (lang.equals("en")){

                String[] words = text.split(" ");
                hashtags = getHastags(words);
                positive = getPolarityWords(words, positiveWords);
                negative = getPolarityWords(words, negativeWords);

                hashTagPolarity.setPositive(positive);
                hashTagPolarity.setNegative(negative);
                hashTagPolarity.setWords((long) words.length);

                for (String hash:hashtags){

                    if (hash.length()>1){
                        HashTagPair hashtag = new HashTagPair();
                        hashtag.setHashtag(new Text(hash));
                        context.write(hashtag, hashTagPolarity);  //Write map result {word,1}
                    }

                }
            }

        } catch (JSONException e) {
        }

    }


    private void readFile(String filePath) {
        try{
            System.out.println("[StopWords] READING FILE --> " + filePath);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
            String stopWord = null;
            while((stopWord = bufferedReader.readLine()) != null) {

                if (filePath.contains("positive")){
                    positiveWords.add(stopWord.toLowerCase());
                }else{
                    negativeWords.add(stopWord.toLowerCase());
                }


            }
        } catch(IOException ex) {
            System.err.println("[StopWords] Exception while reading stop words file: " + ex.getMessage());
        }
    }

    private Double getPolarityWords(String[] words, Set<String> polarityWords) {

        Double polarityCount = 0.0;

        for (String word: words){

            if (polarityWords.contains(word)){
                polarityCount += 1;
            }
        }
        return polarityCount;
    }


    private List<String> getHastags(String[] words) {

        HashSet<String> hastags = new HashSet<String>();

        for (String word: words){
            if (word.length()>0 && word.charAt(0) =='#'){
                hastags.add(word);
            }
        }

        return new ArrayList<String>(hastags);
    }

}