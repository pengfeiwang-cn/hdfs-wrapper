import luxor.hdfs.fs.PipedFileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class TestCreate extends TestCaseBase {

    @Test
    public void go() throws IOException, InterruptedException {
        doTest(this.getClass().getCanonicalName());
    }

    public void run() {
        PipedFileSystem fs = new PipedFileSystem();
        try {
            Path p = new Path("/tmp/test/file1");
            fs.create(p);
            FileStatus status = fs.getFileStatus(p);
            assertFalse("assert create & getFileStatus.", status == null);
        } catch (IOException e) {
            logger.error(e);
            System.exit(-1);
        }
    }
}