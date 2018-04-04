package luxor.hdfs.common.commands;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class OpenCommand extends Pipeable {
    private String path;
    private String namedPipe;
    private int bufferSize;

    public OpenCommand() {}

    public OpenCommand(@NotNull String path, @NotNull String namedPipe, int bufferSize) {
        this.path = path;
        this.namedPipe = namedPipe;
        this.bufferSize = bufferSize;
    }

    @Override
    public int getType() {
        return Pipeable.OPEN;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, path);
        StreamUtils.writeString(output, namedPipe);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        path = StreamUtils.readString(input);
        namedPipe = StreamUtils.readString(input);
    }

    public String getPath() {
        return path;
    }

    public String getNamedPipe() {
        return namedPipe;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
