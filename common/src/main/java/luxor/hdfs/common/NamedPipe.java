package luxor.hdfs.common;

import com.sun.istack.internal.NotNull;

public class NamedPipe {
    static {
        System.loadLibrary("luxorpipe");
    }

    // return null means succeed.
    private static native String createPipeInternal(String path);

    public static void createPipe(@NotNull String path) {
        String err = createPipeInternal(path);
        if (err != null) {
            throw new RuntimeException(String.format("Create named pipe '%s' failed.", path));
        }
    }
}
