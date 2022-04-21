package Job2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class VentesMapper2 extends MapReduceBase implements Mapper<LongWritable, Text,Text,LongWritable> {

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, LongWritable> outputCollector, Reporter reporter) throws IOException {
        String Ventes[]=text.toString().split(" ");
        String key=Ventes[0]+" "+Ventes[1];
        outputCollector.collect( new Text(key), new LongWritable(Long.valueOf(Ventes[3])));
    }
}

