import Models.HashTagPair;
import Models.HashTagPolarity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Marc on 2/6/16.
 */
public class HasTagCombiner extends Reducer<HashTagPair, HashTagPolarity, HashTagPair, HashTagPolarity>
{

    private Map<HashTagPair, HashTagPolarity> topHasTags = new HashMap<>();

    Long topN = Long.valueOf(20);

    @Override
    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        topN = conf.getLong("TopN",0)*2;

    }

    public void reduce(HashTagPair key, Iterable<HashTagPolarity> values, Context context)
            throws IOException, InterruptedException
    {



        HashTagPolarity finalPolarity = new HashTagPolarity();
        HashTagPair finalKey = new HashTagPair();

        long count = key.getCount().get();

        finalPolarity.setName(key.getHashtag().toString());

        for (HashTagPolarity val : values) {
            count++;
            finalPolarity.plusNegative(val.getNegative());
            finalPolarity.plusPositive(val.getPositive());
            finalPolarity.plusWords(val.getWords());
            key.plusCount();
        }


        finalKey.setCount(new LongWritable(count));
        finalKey.setHashtag(new Text(key.getHashtag().toString()));

        //context.write(key.getHashtag(),finalPolarity.getPolarity());
        saveTopHashTag(finalKey,finalPolarity);

    }

    private void saveTopHashTag(HashTagPair key, HashTagPolarity finalPolarity) {

        HashTagPair minorHasTagCount = getMinorCount();

        if (minorHasTagCount == null){
            topHasTags.put(key,finalPolarity);
        }else if(topHasTags.size()==topN){
            if(minorHasTagCount.getCount().get() < key.getCount().get()){
                topHasTags.remove(minorHasTagCount);
                topHasTags.put(key, finalPolarity);
            }
        }else{
            topHasTags.put(key, finalPolarity);
        }

    }


    private HashTagPair getMinorCount(){
        HashTagPair minor = null;

        for (HashTagPair key : topHasTags.keySet()) {
            if (minor==null){
                minor = key;
            }else if(minor.getCount().get()>key.getCount().get()){
                minor = key;
            }
        }

        return minor;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

        for (HashTagPair key : topHasTags.keySet()) {

            String output = String.valueOf(topHasTags.get(key).getPolarity().get()) + "\t" +
                            String.valueOf(topHasTags.get(key).getPositive()) + "\t" +
                            String.valueOf(topHasTags.get(key).getNegative());


            context.write(key, topHasTags.get(key));
        }

    }
}
