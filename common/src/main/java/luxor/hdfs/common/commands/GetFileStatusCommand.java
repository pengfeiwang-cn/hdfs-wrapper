package luxor.hdfs.common.commands;

import com.sun.istack.NotNull;
import luxor.hdfs.common.StreamUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GetFileStatusCommand extends Pipeable {
    private Path path;

    public GetFileStatusCommand() {}

    public GetFileStatusCommand(@NotNull Path path) {
        this.path = path;
    }

    @Override
    public int getType() {
        return Pipeable.GETFILESTATUS;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, path.toString());
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        path = new Path(StreamUtils.readString(input));
    }

    public Path getPath() {
        return path;
    }
}
