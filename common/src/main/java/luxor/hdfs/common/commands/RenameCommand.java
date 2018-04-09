package luxor.hdfs.common.commands;

import com.sun.istack.NotNull;
import luxor.hdfs.common.StreamUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RenameCommand extends Pipeable {
    private Path src;
    private Path dst;

    public RenameCommand() {}

    public RenameCommand(@NotNull Path src, @NotNull Path dst) {
        this.src = src;
        this.dst = dst;
    }

    @Override
    public int getType() {
        return Pipeable.RENAME;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, src.toString());
        StreamUtils.writeString(output, dst.toString());
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        src = new Path(StreamUtils.readString(input));
        dst = new Path(StreamUtils.readString(input));
    }

    public Path getSrc() {
        return src;
    }

    public Path getDst() {
        return dst;
    }

    @Override
    public String toString() {
        return String.format("RenameCommand:{source='%s', destination='%s'}", src, dst);
    }
}
