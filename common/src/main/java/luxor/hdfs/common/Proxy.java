package luxor.hdfs.common;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class Proxy {
    protected static final int MAX_BUFFER_SIZE = 8192;
    protected byte[] buffer = new byte[MAX_BUFFER_SIZE];
    protected static final Logger logger = Logger.getLogger(Proxy.class);

    private InputStream input;
    private OutputStream output;

    public InputStream getInput() {
        return input;
    }

    public void setInput(InputStream input) {
        this.input = input;
    }

    public OutputStream getOutput() {
        return output;
    }

    public void setOutput(OutputStream output) {
        this.output = output;
    }

    public abstract void close() throws IOException;
}
