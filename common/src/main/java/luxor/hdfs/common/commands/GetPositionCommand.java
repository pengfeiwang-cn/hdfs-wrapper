package luxor.hdfs.common.commands;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GetPositionCommand extends Pipeable {
    private String namedPipe;

    public GetPositionCommand() {}

    public GetPositionCommand(@NotNull String namedPipe) {
        this.namedPipe = namedPipe;
    }

    @Override
    public int getType() {
        return Pipeable.GETPOSITION;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, namedPipe);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        namedPipe = StreamUtils.readString(input);
    }
}
