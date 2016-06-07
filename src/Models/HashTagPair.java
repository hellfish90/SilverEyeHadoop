package Models;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Marc on 31/5/16.
 */
public class HashTagPair implements WritableComparable<HashTagPair> {

    Text hashtag = new Text("");
    LongWritable count = new LongWritable(2);

    public Text getHashtag() {
        return hashtag;
    }

    public void setHashtag(Text hashtag) {
        this.hashtag = hashtag;
    }

    public LongWritable getCount() {
        return count;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }

    public void plusCount(){
        count = new LongWritable(count.get()+1);
    }

    public void write(DataOutput dataOutput) throws IOException {
        hashtag.write(dataOutput);
        count.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        hashtag.readFields(dataInput);
        count.readFields(dataInput);
    }

    @Override
    public int compareTo(HashTagPair o) {
        return getHashtag().compareTo(o.getHashtag());
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HashTagPair that = (HashTagPair) o;

        return hashtag.equals(that.hashtag);

    }

    @Override
    public int hashCode() {
        return hashtag.hashCode();
    }
}
