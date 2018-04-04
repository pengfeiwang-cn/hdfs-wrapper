package luxor.hdfs.common;

import com.sun.istack.internal.NotNull;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class NamedPipe {
    static {
        System.load("/tmp/libluxorpipe.so");
    }

    private static final Logger logger = Logger.getLogger(NamedPipe.class);
    private static final ExecutorService pool = Executors.newCachedThreadPool();

    // return null means succeed.
    private static native String createPipeInternal(String path);

    public static void createPipe(@NotNull String path) throws IOException {
        String err = createPipeInternal(path);
        if (err != null) {
            throw new IOException(String.format("Create named pipe '%s' failed, error:%s.", path, err));
        }
    }

    public static Future<FileInputStream> createInputStreamAsync(final String namedPipe) {
        return pool.submit(new Callable<FileInputStream>() {
            public FileInputStream call() {
                try {
                    return new FileInputStream(namedPipe);
                } catch (FileNotFoundException e) {
                    logger.error("createInputStreamAsync failed.", e);
                    return null;
                }
            }
        });
    }

    public static Future<FileOutputStream> createOutputStreamAsync(final String namedPipe) {
        return pool.submit(new Callable<FileOutputStream>() {
            public FileOutputStream call() {
                try {
                    return new FileOutputStream(namedPipe);
                } catch (FileNotFoundException e) {
                    logger.error("createInputStreamAsync failed.", e);
                    return null;
                }
            }
        });
    }
}
