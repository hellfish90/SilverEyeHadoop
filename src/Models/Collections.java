package Models;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by Marc on 30/6/16.
 */
public class Collections implements Writable, Serializable {

    MapWritable positiveCollections, negativeCollections, neutralCollections, counterCollection;

    public Collections(){
        positiveCollections = new MapWritable();
        negativeCollections = new MapWritable();
        neutralCollections = new MapWritable();
        counterCollection = new MapWritable();
    }

    public void plusPositiveCollection(String collection, Long carriedValue){

        if (positiveCollections.get(new Text(collection))!=null){
            LongWritable oldValue = (LongWritable)positiveCollections.get(new Text(collection));
            positiveCollections.put(new Text(collection), new LongWritable(carriedValue+oldValue.get()));

        }else{
            positiveCollections.put(new Text(collection), new LongWritable(carriedValue));
        }
    }

    public void plusNegativeCollection(String collection, Long carriedValue){

        if (negativeCollections.get(new Text(collection))!=null){
            LongWritable oldValue = (LongWritable)negativeCollections.get(new Text(collection));
            negativeCollections.put(new Text(collection), new LongWritable(carriedValue+oldValue.get()));

        }else{
            negativeCollections.put(new Text(collection), new LongWritable(carriedValue));
        }
    }

    public void plusNeutralCollection(String collection, Long carriedValue){

        if (neutralCollections.get(new Text(collection))!=null){
            LongWritable oldValue = (LongWritable)neutralCollections.get(new Text(collection));
            neutralCollections.put(new Text(collection), new LongWritable(carriedValue+oldValue.get()));

        }else{
            neutralCollections.put(new Text(collection), new LongWritable(carriedValue));
        }
    }

    public void plusCounterCollection(String collection, Long carriedValue){

        if (counterCollection.get(new Text(collection))!=null){
            LongWritable oldValue = (LongWritable)counterCollection.get(new Text(collection));
            counterCollection.put(new Text(collection), new LongWritable(carriedValue+oldValue.get()));

        }else{
            counterCollection.put(new Text(collection), new LongWritable(carriedValue));
        }
    }

    public MapWritable getPositiveCollections() {
        return positiveCollections;
    }

    public MapWritable getNegativeCollections() {
        return negativeCollections;
    }

    public MapWritable getNeutralCollections() {
        return neutralCollections;
    }

    public MapWritable getCounterCollection() {
        return counterCollection;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        positiveCollections.write(dataOutput);
        negativeCollections.write(dataOutput);
        neutralCollections.write(dataOutput);
        counterCollection.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        positiveCollections.readFields(dataInput);
        negativeCollections.readFields(dataInput);
        neutralCollections.readFields(dataInput);
        counterCollection.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "Collections{" +
                "positiveCollections=" + positiveCollections.entrySet().toString() +
                ", negativeCollections=" + negativeCollections.entrySet().toString() +
                ", neutralCollections=" + neutralCollections.entrySet().toString() +
                ", counterCollection=" + counterCollection.entrySet().toString() +
                '}';
    }
}
