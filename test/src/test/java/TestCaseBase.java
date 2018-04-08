import luxor.hdfs.parent.Driver;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public abstract class TestCaseBase extends Assert implements Runnable{
    protected static final Logger logger = Logger.getLogger(TestCaseBase.class);

    @Rule
    public ExpectedException expected = ExpectedException.none();

    public void doTest(String className) throws IOException, InterruptedException {
        Driver driver = new Driver("/usr/bin/java",
                "-cp",
                System.getProperty("java.class.path"),
                "ChildProc",
                className);
        assertEquals(0, driver.Run());
    }

    @Test
    public void go() throws IOException, InterruptedException {
        doTest(this.getClass().getCanonicalName());
    }
}
