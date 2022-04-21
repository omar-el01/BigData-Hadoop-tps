package Job1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class VentesMapper extends MapReduceBase implements Mapper<IntWritable, Text,Text,IntWritable> {

 @Override
 public void map(IntWritable intWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
String Ventes[]=text.toString().split(" ");
 outputCollector.collect(new Text(Ventes[1]),new IntWritable(Integer.parseInt(Ventes[3])));
}
}
