package luxor.hdfs.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class Proxy {
    protected static final int MAX_BUFFER_SIZE = 8192;
    protected byte[] buffer = new byte[MAX_BUFFER_SIZE];

    protected void readThenWrite(int length, InputStream input, OutputStream output) throws IOException {
        int alreadyRead = 0;
        while (alreadyRead < length) {
            int left = length - alreadyRead;
            int rl = input.read(buffer, 0, MAX_BUFFER_SIZE >= left ? left : MAX_BUFFER_SIZE);
            if (rl == -1) { // EOF
                input.close();
                output.close();
                break;
            }

            output.write(buffer, 0, rl);
            alreadyRead += rl;
        }
    }

    public abstract void close() throws IOException;
}
