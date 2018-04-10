package luxor.hdfs.common;

import org.junit.Assert;

import java.io.File;

public class NamedPipeTest extends Assert {

    @org.junit.Test
    public void testCreatePipe() throws Exception {
        String path = "/tmp/abc";
        NamedPipe.createPipe(path); //no exception means OK.

        assertTrue((new File(path)).delete());
    }
}