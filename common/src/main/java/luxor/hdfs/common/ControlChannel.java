package luxor.hdfs.common;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.commands.Pipeable;
import luxor.hdfs.common.commands.PipedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ControlChannel {
    private InputStream input;
    private OutputStream output;

    public ControlChannel(@NotNull InputStream input, @NotNull OutputStream output) {
        this.input = input;
        this.output = output;
    }

    /*
     * 这个方法会导致子进程中的所有指令都串行执行.不做优化先.
     */
    public void sendCommand(Pipeable cmd)
            throws IOException {
        synchronized (this) {
            Pipeable.serialize(cmd, output);
            Pipeable result = waitCommand();
            if (result instanceof PipedException) {
                throw new IOException(((PipedException)result).getMessage());
            }
        }
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
            return null;
        }
    }
}
