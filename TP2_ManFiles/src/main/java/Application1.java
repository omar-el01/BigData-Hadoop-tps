import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Application1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
        System.setProperty("HADOOP_USER_NAME", "root");
 try {
     FileSystem fs=FileSystem.get(conf);
     Path Cours1CPP = new Path("/BDCC/CPP/Cours/Cours1CPP"),
             Cours2CPP = new Path("/BDCC/CPP/Cours/Cours2CPP"),
             Cours3CPP = new Path("/BDCC/CPP/Cours/Cours3CPP");
     BufferedWriter bw;
     FSDataOutputStream fsdatainput;
     fsdatainput = fs.create(Cours1CPP);
        bw = new BufferedWriter(new OutputStreamWriter(fsdatainput));
        bw.write("adding contenu to Cours1CPP  ");
        bw.newLine();
        bw.write("Cours1CPP !");
        bw.close();
     fsdatainput.close();
        System.out.println("Cours1CPP file created : /BDCC/CPP/Cours/Cours1CPP ✅");


     fsdatainput = fs.create(Cours2CPP);
        bw = new BufferedWriter(new OutputStreamWriter(fsdatainput));
        bw.write("adding contenu to Cours2CPP  ");
        bw.newLine();
        bw.write("Cours2CPP !");
        bw.close();
     fsdatainput.close();
        System.out.println("Cours2CPP file created : /BDCC/CPP/Cours/Cours2CPP ✅");

     fsdatainput = fs.create(Cours3CPP);
        bw = new BufferedWriter(new OutputStreamWriter(fsdatainput));
        bw.write("adding contenu to Cours3CPP ");
        bw.newLine();
        bw.write("Cours1CPP ! ");
        bw.close();
     fsdatainput.close();
        System.out.println("Cours2CPP file created : /BDCC/CPP/Cours/Cours3CPP ✅");

    }catch(Exception exc){
        System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
        exc.printStackTrace();
    }


}

}
