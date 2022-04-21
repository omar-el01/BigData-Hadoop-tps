package Exercice2;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class TemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text,Text, DoubleWritable> {

    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, DoubleWritable> outputCollector, Reporter reporter) throws IOException {
        if(longWritable.get()==0)return;
        CSVParser parser = new CSVParser();
        String data[] = parser.parseLine(text.toString());

        String [] date = data[1].split("-");
        String doubm= data[13].replace('"', ' ').replace(',', '.').trim();
        double temp = Double.valueOf( doubm );
    outputCollector.collect( new Text( date[1] ), new DoubleWritable( temp ) );
    }
}
