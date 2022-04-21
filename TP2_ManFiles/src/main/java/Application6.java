import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Application6 {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        conf.set("fs.replication","1");
        System.setProperty("HADOOP_USER_NAME", "root");

        try {

            FileSystem fs = FileSystem.get(conf);

            Path TP1JAVA = new Path("/BDCC/JAVA/TPs/TP1JAVA"),
                    TP2JAVA = new Path("/BDCC/JAVA/TPs/TP2JAVA"),
                    TP3JAVA = new Path("/BDCC/JAVA/TPs/TP3JAVA");
            Path TP1javalocal = new Path("./Q7/TP1JAVA"),
                    TP2javalocal = new Path("./Q7/TP2JAVA"),
                    TP3javalocal = new Path("./Q7/TP3JAVA");

            fs.copyFromLocalFile(TP1javalocal, TP1JAVA);

            fs.copyFromLocalFile(TP2javalocal, TP2JAVA);

            fs.copyFromLocalFile(TP3javalocal, TP3JAVA);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
