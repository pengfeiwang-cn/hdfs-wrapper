package luxor.hdfs.common.commands;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SeekCommand extends Pipeable {
    private String namedPipe;
    private long desired;

    public SeekCommand() {}

    public SeekCommand(@NotNull String namedPipe, long desired) {
        this.namedPipe = namedPipe;
        this.desired = desired;
    }

    @Override
    public int getType() {
        return Pipeable.READ;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, namedPipe);
        StreamUtils.writeLong(output, desired);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        namedPipe = StreamUtils.readString(input);
        desired = StreamUtils.readLong(input);
    }

    public String getNamedPipe() {
        return namedPipe;
    }

    public long getDesired() {
        return desired;
    }

    @Override
    public String toString() {
        return String.format("SeekCommand:{namedPipe='%s', desired=%s}", namedPipe, desired);
    }
}
