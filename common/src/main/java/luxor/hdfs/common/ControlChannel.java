package luxor.hdfs.common;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.commands.Pipeable;
import luxor.hdfs.common.commands.PipedException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ControlChannel {
    private static final Logger logger = Logger.getLogger(ControlChannel.class);

    private InputStream input;
    private OutputStream output;

    public ControlChannel(@NotNull InputStream input, @NotNull OutputStream output) {
        this.input = input;
        this.output = output;
    }

    /*
     * 这个方法会导致子进程中的所有指令都串行执行.不做优化先.
     */
    public void sendRequest(Pipeable cmd) throws IOException {
        synchronized (this) {
            logger.info(String.format("Sending Command - %s", cmd));
            Pipeable.serialize(cmd, output);
            Pipeable result = waitCommand();
            logger.info(String.format("Got result - %s", result));
            if (result instanceof PipedException) {
                throw new IOException(((PipedException)result).getMessage());
            }
        }
    }

    public void sendResponse(Pipeable res) throws IOException {
        Pipeable.serialize(res, output);
    }

    public InputStream getInput() {
        return input;
    }

    public OutputStream getOutput() {
        return output;
    }

    // NULL means no more commands.
    public Pipeable waitCommand() {
        try {
            return Pipeable.deserialize(input);
        }
        catch (IOException e) {
            logger.warn("When waitCommand in control channel, exception occured.", e);
            return null;
        }
    }
}
