package luxor.hdfs.parent.proxies;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.Proxy;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

public class OutProxy extends Proxy {
    private FSDataOutputStream output;
    private InputStream dataOutputChannel;

    public OutProxy(@NotNull FSDataOutputStream output, @NotNull InputStream dataOutputChannel)
            throws FileNotFoundException {
        this.output = output;
        this.dataOutputChannel = dataOutputChannel;
        setInput(dataOutputChannel);
        setOutput(output);
    }

    @Override
    public void close() throws IOException {
        if (output != null) {
            output.close();
            output = null;
        }

        if (dataOutputChannel != null) {
            dataOutputChannel.close();
            dataOutputChannel = null;
        }
    }

    public long getPos() throws IOException {
        return output.getPos();
    }

    public void hflush() throws IOException {
        output.hflush();
    }

    public void hsync() throws IOException {
        output.hsync();
    }

    public void setDropBehind(boolean dropBehind) throws IOException {
        output.setDropBehind(dropBehind);
    }

    public void write(int length) throws IOException {
        readThenWrite(length);
    }

    public FSDataOutputStream getOutput() {
        return output;
    }

    public InputStream getDataOutputChannel() {
        return dataOutputChannel;
    }

    private  void readThenWrite(int length) throws IOException {
        int alreadyRead = 0;
        while (alreadyRead < length) {
            int left = length - alreadyRead;
            int rl = getInput().read(buffer, 0, MAX_BUFFER_SIZE >= left ? left : MAX_BUFFER_SIZE);
            if (rl == -1) { // EOF, it must be something wrong
                throw new RuntimeException(
                        String.format("In OutProxy, %s bytes should be read, but got %s.", length, alreadyRead));
            }
            output.write(buffer, 0, rl);
            alreadyRead += rl;
        }
    }
}
