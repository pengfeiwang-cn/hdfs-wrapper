package luxor.hdfs.common.commands;

import com.sun.istack.NotNull;
import luxor.hdfs.common.StreamUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class AppendCommand extends Pipeable {
    private Path path;
    private int bufferSize;

    public AppendCommand() {}

    public AppendCommand(@NotNull Path path, int bufferSize) {
        this.path = path;
        this.bufferSize = bufferSize;
    }

    @Override
    public int getType() {
        return Pipeable.APPEND;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, path.toString());
        StreamUtils.writeInt(output, bufferSize);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        path = new Path(StreamUtils.readString(input));
        bufferSize = StreamUtils.readInt(input);
    }

    public Path getPath() {
        return path;
    }

    public int getBufferSize() {
        return bufferSize;
    }
}
