package luxor.hdfs.common;

import com.sun.istack.internal.NotNull;

import java.io.IOException;

public class NamedPipe {
    static {
        System.load("/tmp/libluxorpipe.so");
    }

    // return null means succeed.
    private static native String createPipeInternal(String path);

    public static void createPipe(@NotNull String path) throws IOException {
        String err = createPipeInternal(path);
        if (err != null) {
            throw new IOException(String.format("Create named pipe '%s' failed, error:%s.", path, err));
        }
    }
}
