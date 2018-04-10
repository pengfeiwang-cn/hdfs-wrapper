package luxor.hdfs.parent.proxies;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.Proxy;
import luxor.hdfs.common.StreamUtils;
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
        setInput(input);
        setOutput(dataInputChannel);
    }

    public long getPos() throws IOException {
        return input.getPos();
    }

    public void read(int length) throws IOException {
        readThenWrite(length);
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

    protected void readThenWrite(int length) throws IOException {
        assert (getInput() != null && getOutput() != null);

        logger.info("readThenWrite - ing.");

        int rl = input.read(buffer, 0, MAX_BUFFER_SIZE >= length ? length : MAX_BUFFER_SIZE);
        logger.info(String.format("input.read %s bytes.", rl));
        if (rl == -1) { // EOF
            StreamUtils.writeInt(getOutput(), -1);
        }
        else {
            StreamUtils.writeInt(getOutput(), rl);
            getOutput().write(buffer, 0, rl);
        }

        logger.info("readThenWrite - ed.");
    }

}
