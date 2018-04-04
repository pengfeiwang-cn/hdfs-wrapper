package luxor.hdfs.common.commands;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CreateCommand extends Pipeable {
    private String path;
    private String namedPipe;
    private boolean overwrite;
    private int bufferSize;
    private short replication;
    private long blockSize;

    public CreateCommand() {}

    public CreateCommand(@NotNull String path,
                         @NotNull String namedPipe,
                         boolean overwrite,
                         int bufferSize,
                         short replication,
                         long blockSize) {
        this.path = path;
        this.namedPipe = namedPipe;
        this.overwrite = overwrite;
        this.bufferSize = bufferSize;
        this.replication = replication;
        this.blockSize = blockSize;
    }

    @Override
    public int getType() {
        return Pipeable.CREATE;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, path);
        StreamUtils.writeString(output, namedPipe);
        StreamUtils.writeBoolean(output, overwrite);
        StreamUtils.writeInt(output, bufferSize);
        StreamUtils.writeShort(output, replication);
        StreamUtils.writeLong(output, blockSize);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        this.path = StreamUtils.readString(input);
        this.namedPipe = StreamUtils.readString(input);
        this.overwrite = StreamUtils.readBoolean(input);
        this.bufferSize = StreamUtils.readInt(input);
        this.replication = StreamUtils.readShort(input);
        this.blockSize = StreamUtils.readLong(input);
    }

    public String getPath() {
        return path;
    }

    public String getNamedPipe() {
        return namedPipe;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public short getReplication() {
        return replication;
    }

    public long getBlockSize() {
        return blockSize;
    }
}
