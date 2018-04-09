package luxor.hdfs.common.commands;

import com.sun.istack.NotNull;
import luxor.hdfs.common.StreamUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class DeleteCommand extends Pipeable {
    private Path path;
    private boolean recursive;

    public DeleteCommand() {}

    public DeleteCommand(@NotNull Path path, boolean recursive) {
        this.path = path;
        this.recursive = recursive;
    }

    @Override
    public int getType() {
        return Pipeable.DELETE;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, path.toString());
        StreamUtils.writeBoolean(output, recursive);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        path = new Path(StreamUtils.readString(input));
        recursive = StreamUtils.readBoolean(input);
    }

    public Path getPath() {
        return path;
    }

    public boolean isRecursive() {
        return recursive;
    }

    @Override
    public String toString() {
        return String.format("DeleteCommand:{path='%s', recuisive=%s}", path, recursive);
    }
}
