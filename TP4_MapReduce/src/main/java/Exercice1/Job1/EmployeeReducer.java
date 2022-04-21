package Exercice1.Job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.Iterator;

public class EmployeeReducer extends MapReduceBase implements Reducer<Text, DoubleWritable,Text,Text> {

    @Override
    public void reduce(Text text, Iterator<DoubleWritable> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        Double min,max,tmp;
        max =iterator.next().get();
        min =max;
        while( iterator.hasNext()) {
            tmp=iterator.next().get();
            if( tmp> max) max = tmp;
            if( tmp< min)
                min = tmp;
        }
        outputCollector.collect( text, new Text("[ max: "+max+", min: "+min+" ]"));
    }
    }




