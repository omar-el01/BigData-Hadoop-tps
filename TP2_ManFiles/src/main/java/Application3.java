import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class Application3 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
        System.setProperty("HADOOP_USER_NAME", "root");
        try{
            FileSystem fs=FileSystem.get(conf);
            Path  Cours1CPP = new Path("/BDCC/CPP/Cours/Cours1CPP"),
                    Cours2CPP = new Path("/BDCC/CPP/Cours/Cours2CPP"),
                    Cours3CPP = new Path("/BDCC/CPP/Cours/Cours3CPP");
            Path Coursjava =new Path( "/BDCC/JAVA/Cours/");
            FileUtil.copy(fs,Cours1CPP,fs,Coursjava,true,conf);
            FileUtil.copy(fs,Cours2CPP,fs,Coursjava,true,conf);
            FileUtil.copy(fs,Cours3CPP,fs,Coursjava,true,conf);
        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }
}
}
