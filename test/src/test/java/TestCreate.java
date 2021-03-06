import luxor.hdfs.fs.PipedFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public class TestCreate extends TestCaseBase {

    public void run() {
        PipedFileSystem fs = new PipedFileSystem();
        fs.setWorkingDirectory(new Path("/tmp/test/fs"));
        fs.setConf(new Configuration());
        try {
            Path p = new Path("file1");
            fs.create(p);
            FileStatus status = fs.getFileStatus(p);
            assertFalse("assert create & getFileStatus.", status == null);
            assertTrue(fs.delete(p, true));
            System.exit(0);
        } catch (Exception e) {
            logger.error("TestCreate", e);
            System.exit(-1);
        }
    }
}
