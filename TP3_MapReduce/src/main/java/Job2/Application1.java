package Job2;

import Job1.Application;
import Job1.VentesMapper;
import Job1.VentesReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Application1 {
    public static void main(String[] args) throws IOException {
        JobConf conf=new JobConf();
        conf.setJobName("Total des prix par ville dans un annee");
        conf.setJarByClass(Application.class);

        conf.setMapperClass(VentesMapper2.class);
        conf.setReducerClass(VentesReducer2.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(LongWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path (args[1]));

        JobClient.runJob(conf);
    }
}
