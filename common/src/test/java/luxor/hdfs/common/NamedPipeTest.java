package luxor.hdfs.common;

import org.junit.Assert;

import java.nio.file.*;

public class NamedPipeTest extends Assert {

    @org.junit.Test
    public void testCreatePipe() throws Exception {
        String path = "/tmp/abc";
        NamedPipe.createPipe(path); //no exception means OK.

        Files.delete(java.nio.file.FileSystems.getDefault().getPath(path));
    }
}