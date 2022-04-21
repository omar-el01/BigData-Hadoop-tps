package Exercice1.Job1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class EmployeeMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        String employees[] = text.toString().split(",");
        outputCollector.collect(new Text(employees[2]), new DoubleWritable(Double.valueOf(employees[4])));
    }
}
