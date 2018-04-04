import luxor.hdfs.parent.Driver;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.io.IOException;

public abstract class TestCaseBase extends Assert implements Runnable{
    protected static final Logger logger = Logger.getLogger(TestCaseBase.class);

    public void doTest(String className) throws IOException, InterruptedException {
        Driver driver = new Driver(String.format("/usr/bin/java -cp \"%s\" ChildProc %s",
                System.getProperty("java.class.path"),
                className));
        assertEquals(0, driver.Run());
    }
}
