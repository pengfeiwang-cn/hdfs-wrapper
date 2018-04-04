package luxor.hdfs.parent.proxies;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.Proxy;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

// WARNING: it's not thread-safe.
public class InProxy extends Proxy {
    private FSDataInputStream input;
    private OutputStream dataInputChannel;

    public InProxy(@NotNull FSDataInputStream input, @NotNull OutputStream dataInputChannel)
            throws FileNotFoundException {
        this.dataInputChannel = dataInputChannel;
        this.input = input;
    }

    public long getPos() throws IOException {
        return input.getPos();
    }

    public void read(int length) throws IOException {
        readThenWrite(length, input, dataInputChannel);
    }

    public void seek(long desired) throws IOException {
        input.seek(desired);
    }

    public boolean seekToNewSource(long targetPos) throws IOException {
        return input.seekToNewSource(targetPos);
    }

    public void setReadahead(long readahead) throws IOException {
        input.setReadahead(readahead);
    }

    public FSDataInputStream getInput() {
        return input;
    }

    public OutputStream getDataInputChannel() {
        return dataInputChannel;
    }

    @Override
    public void close() throws IOException {
        if (input != null) {
            input.close();
            input = null;
        }

        if (dataInputChannel != null) {
            dataInputChannel.close();
            dataInputChannel = null;
        }
    }
}
