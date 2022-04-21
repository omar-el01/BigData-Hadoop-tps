import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;

import java.net.URI;

public class Application {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        conf.set("dfs.replication", "1");
        System.setProperty("HADOOP_USER_NAME", "root");

        try {
            FileSystem fs = FileSystem.get(conf);

            Path cppCoursPath = new Path("/BDCC/CPP/Cours"),
                    cppTpsPath = new Path("/BDCC/CPP/TPs");
            Path javaTpsPath = new Path("/BDCC/JAVA/TPs"),
                    javaCoursPath = new Path("/BDCC/JAVA/Cours");

            if (fs.mkdirs(cppCoursPath)) {
                System.out.println("cppCoursPath path created : /BDCC/CPP/Cours ");
            } else {
                System.out.println("cppCoursPath  can't create path : /BDCC/CPP/Cours ");
            }
            if( fs.mkdirs(cppTpsPath) )
                System.out.println("cppTpPath path created : /BDCC/CPP/TPs ✅");
            else
                System.out.println("cppTpsPath can't create path : /BDCC/CPP/TPs ❌");

            if( fs.mkdirs(javaCoursPath) )
                System.out.println("javaCoursPath path created : /BDCC/JAVA/Cours ✅");
            else
                System.out.println("javaCoursPath  can't create path : /BDCC/JAVA/Cours ❌");

            if( fs.mkdirs(javaTpsPath) )
                System.out.println("javaTpspaths Path  created : /BDCC/JAVA/TPs ✅");
            else
                System.out.println("javaTpspath  can't create path : /BDCC/JAVA/TPs ❌");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
