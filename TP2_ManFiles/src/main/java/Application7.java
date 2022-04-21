import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

public class Application7 {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        conf.set("fs.replication","1");
        System.setProperty("HADOOP_USER_NAME", "root");
        try{
            FileSystem fs = FileSystem.get(conf);
            Path path=new Path("/BDCC");
            RemoteIterator<LocatedFileStatus> it = fs.listFiles(path,true);
            while (it.hasNext()){
                LocatedFileStatus lfs=it.next();
                System.out.println(lfs.getPath().toString());
            }
            fs.close();
        }catch(Exception exc){
            System.err.println("!!O0ops Exception!! \n => "+exc.getMessage());
            exc.printStackTrace();
        }
    }
}
