package Exercice1.Job2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


import java.io.IOException;

import org.apache.hadoop.io.Text;


        public class Application1 {

            public static void main(String[] args) throws IOException {
                JobConf conf=new JobConf();
                conf.setJobName("Max & Min salaries");
                conf.setJarByClass(Exercice1.Job1.Application.class);

                conf.setMapperClass(EmployeMapper1.class);
                conf.setReducerClass(EmployesReducer1.class);

                conf.setMapOutputKeyClass( Text.class);
                conf.setMapOutputValueClass(IntWritable.class);


                conf.setOutputKeyClass(Text.class);
                conf.setOutputValueClass(Text.class);

                conf.setInputFormat(TextInputFormat.class);
                conf.setOutputFormat(TextOutputFormat.class);

                FileInputFormat.addInputPath(conf,new Path(args[0]));
                FileOutputFormat.setOutputPath(conf,new Path(args[1]));
                JobClient.runJob(conf);
            }
        }
