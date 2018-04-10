import luxor.hdfs.fs.PipedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class TestSeek extends TestCaseBase {
    public void run() {
        PipedFileSystem fs = new PipedFileSystem();
        fs.setWorkingDirectory(new Path("/tmp/test/fs"));
        fs.setConf(new Configuration());
        try {
            Path p = new Path("file1");
            FSDataOutputStream out = fs.create(p);
            String baby = "1234567890";
            out.write(baby.getBytes());
            out.hsync();
            out.close();

            FSDataInputStream in = fs.open(p);
            in.seek(3);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String another = br.readLine();
            logger.info(String.format("TestCreateAndWrite - another: '%s'", another));
            assertTrue(another.equals("4567890"));
            br.close();
            in.close();

            assertTrue(fs.delete(p, true));

            System.exit(0);
        } catch (Exception e) {
            logger.error("TestCreateAndWrite", e);
            System.exit(-1);
        }
    }
}
