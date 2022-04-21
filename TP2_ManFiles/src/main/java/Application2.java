import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;

public class Application2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
        System.setProperty("HADOOP_USER_NAME", "root");
        BufferedReader br;
        FSDataInputStream fsdatainput;
        String contenu="";

        try{
            FileSystem fs=FileSystem.get(conf);
            Path Cours1CPP = new Path("/BDCC/CPP/Cours/Cours1CPP"),
                    Cours2CPP = new Path("/BDCC/CPP/Cours/Cours2CPP"),
                    Cours3CPP = new Path("/BDCC/CPP/Cours/Cours3CPP");


            fsdatainput= fs.open(Cours1CPP);
            br = new BufferedReader(new InputStreamReader(fsdatainput));
            System.out.println("\n\n[start] content of the file : /BDCC/CPP/Cours/Cours1CPP : ");
            while( (contenu= br.readLine())!=null ){
                System.out.println(contenu);
            }
            br.close();
            fsdatainput.close();
            System.out.println("[end] content of the file : /BDCC/CPP/Cours/Cours1CPP ------ ");


            fsdatainput = fs.open(Cours2CPP);
            br = new BufferedReader(new InputStreamReader(fsdatainput));
            System.out.println("\n\n[start] content of the file : /BDCC/CPP/Cours/Cours2CPP : ");
            while( (contenu = br.readLine())!=null ){
                System.out.println(contenu);
            }
            br.close();
            fsdatainput.close();
            System.out.println("[end] content of the file : /BDCC/CPP/Cours/Cours2CPP ------ ");


            fsdatainput = fs.open(Cours3CPP);
            br = new BufferedReader(new InputStreamReader(fsdatainput));
            System.out.println("\n\n[start] content of the file : /BDCC/CPP/Cours/Cours3CPP : ");
            while( (contenu = br.readLine())!=null ){
                System.out.println(contenu);
            }
            br.close();
            fsdatainput.close();
            System.out.println("[end] content of the file : /BDCC/CPP/Cours/Cours3CPP ------");


        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }

}
