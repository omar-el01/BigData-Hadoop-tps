package Job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

    public class VentesReducer2 extends MapReduceBase implements Reducer<Text, LongWritable,Text,LongWritable> {

        @Override
        public void reduce(Text text, Iterator<LongWritable> iterator, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
            Long Totalprix= Long.valueOf(0);
            while(iterator.hasNext()){
                Totalprix+=iterator.next().get();
            }
            outputCollector.collect(text,new LongWritable(Totalprix));
        }
    }
