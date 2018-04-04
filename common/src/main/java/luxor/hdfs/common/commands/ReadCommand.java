package luxor.hdfs.common.commands;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ReadCommand extends Pipeable {
    private String namedPipe;
    private int length;

    public ReadCommand() {}

    public ReadCommand(@NotNull String namedPipe, int length) {
        this.namedPipe = namedPipe;
        this.length = length;
    }

    @Override
    public int getType() {
        return Pipeable.READ;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, namedPipe);
        StreamUtils.writeInt(output, length);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        namedPipe = StreamUtils.readString(input);
        length = StreamUtils.readInt(input);
    }

    public String getNamedPipe() {
        return namedPipe;
    }

    public int getLength() {
        return length;
    }
}
