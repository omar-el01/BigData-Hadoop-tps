import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Application4 {

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
        System.setProperty("HADOOP_USER_NAME", "root");

        try {
            FileSystem fs = FileSystem.get(conf);
            Path Cours1CPP = new Path("/BDCC/JAVA/Cours/Cours1CPP"),
                    Cours2CPP = new Path("/BDCC/JAVA/Cours/Cours2CPP"),
                    Cours3CPP = new Path("/BDCC/JAVA/Cours/Cours3CPP"),
                    Cours1JAVA = new Path("/BDCC/JAVA/Cours/Cours1JAVA"),
                    Cours2JAVA = new Path("/BDCC/JAVA/Cours/Cours2JAVA");

            fs.rename(Cours1CPP, Cours1JAVA);

            fs.rename(Cours2CPP, Cours2JAVA);

            fs.delete(Cours3CPP, true);

        } catch (Exception exc) {
            System.err.println("!!O0ops Exception!! \n => " + exc.getMessage());
            exc.printStackTrace();
        }
    }
}
