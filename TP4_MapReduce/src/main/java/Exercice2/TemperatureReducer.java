package Exercice2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

public class TemperatureReducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,Text> {


    @Override
    public void reduce(Text text, Iterator<DoubleWritable> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        Double Max,Min, tmp;
        Max=iterator.next().get();
        Min=Max;
        while (iterator.hasNext()) {
            tmp = iterator.next().get();
            if (tmp > Max)
                Max = tmp;
            if (tmp< Min)
                Min= tmp;
        }
        outputCollector.collect(text, new Text("[ max: " + Max + ", min: " + Min + " ]"));
    }
    }
