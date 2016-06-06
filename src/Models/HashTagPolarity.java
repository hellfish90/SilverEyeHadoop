package Models;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Marc on 30/5/16.
 */

public class HashTagPolarity implements Writable {

    Text name = new Text("");
    DoubleWritable positive=new DoubleWritable(0.0), negative=new DoubleWritable(0.0);
    DoubleWritable positiveRatio =new DoubleWritable(0.0), negativeRatio=new DoubleWritable(0.0);
    DoubleWritable polarity = new DoubleWritable(0.0);
    LongWritable words= new LongWritable(0);
    LongWritable count = new LongWritable(1);


    public DoubleWritable getPolarity() {
        return polarity;
    }

    public Text getName() {
        return name;
    }

    public void setName(String name) {
        this.name = new Text(name);
    }

    public Double getPositive() {
        return positive.get();
    }

    public void setPositive(Double positive) {
        this.positive = new DoubleWritable(positive);
    }

    public Double getNegative() {
        return negative.get();
    }

    public void setNegative(Double negative) {
        this.negative = new DoubleWritable(negative);
    }

    public Long getWords() {
        return words.get();
    }

    public void setWords(Long words) {
        this.words = new LongWritable(words);
    }

    public void plusPositive(Double pos){
        positive = new DoubleWritable(positive.get()+pos);
    }

    public void plusNegative(Double neg){
        negative = new DoubleWritable(negative.get()+neg);
    }

    public void plusWords(Long word){
        words = new LongWritable(words.get()+word);
    }

    public LongWritable getCount() {
        return count;
    }

    public void setCount(LongWritable count) {
        this.count = count;
    }

    public void calculateRatios(){
        positiveRatio = new DoubleWritable(positive.get()/words.get());
        negativeRatio = new DoubleWritable(negative.get()/words.get());
        polarity = new DoubleWritable(positiveRatio.get() - negativeRatio.get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

        name.write(dataOutput);
        positive.write(dataOutput);
        negative.write(dataOutput);
        positiveRatio.write(dataOutput);
        negativeRatio.write(dataOutput);
        polarity.write(dataOutput);
        words.write(dataOutput);
        count.write(dataOutput);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

        name.readFields(dataInput);
        positive.readFields(dataInput);
        negative.readFields(dataInput);
        positiveRatio.readFields(dataInput);
        negativeRatio.readFields(dataInput);
        polarity.readFields(dataInput);
        words.readFields(dataInput);
        count.readFields(dataInput);
    }
}
