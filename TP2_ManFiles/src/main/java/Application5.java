import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Application5 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
        System.setProperty("HADOOP_USER_NAME", "root");
        try{
            FileSystem fs = FileSystem.get(conf);
            Path TP1CPP = new Path("/BDCC/CPP/TPs/TP1CPP"),
                    TP2CPP = new Path("/BDCC/CPP/TPs/TP2CPP");
            Path TP1cpplocal = new Path("./Q7/TP1CPP"),
                    TP2cpplocal = new Path("./Q7/TP2CPP");

            fs.copyFromLocalFile( TP1cpplocal, TP1CPP);

            fs.copyFromLocalFile( TP2cpplocal, TP2CPP);

        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }


    }
    }

