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
        readThenWrite(length, dataOutputChannel, output);
    }

    public FSDataOutputStream getOutput() {
        return output;
    }

    public InputStream getDataOutputChannel() {
        return dataOutputChannel;
    }
}
