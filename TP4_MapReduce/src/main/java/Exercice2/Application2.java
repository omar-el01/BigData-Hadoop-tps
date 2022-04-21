package Exercice2;

import Exercice1.Job1.EmployeeMapper;
import Exercice1.Job1.EmployeeReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

    public class Application2 {

        public static void main(String[] args) throws IOException {
            JobConf conf=new JobConf();
            conf.setJobName("Max & Min salaries");
            conf.setJarByClass(Exercice1.Job1.Application.class);

            conf.setMapperClass(TemperatureMapper.class);
            conf.setReducerClass(TemperatureReducer.class);

            conf.setMapOutputKeyClass( Text.class);
            conf.setMapOutputValueClass( DoubleWritable.class);


            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(Text.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.addInputPath(conf,new Path(args[0]));
            FileOutputFormat.setOutputPath(conf,new Path(args[1]));
            JobClient.runJob(conf);
        }
    }
