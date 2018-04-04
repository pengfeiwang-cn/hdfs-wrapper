package luxor.hdfs.common.commands;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/*
 * 在pipe上传输的异常信息封装, 只有IO Exception.
 */
public class PipedException extends Pipeable {
    private String message;

    public PipedException() {}

    public PipedException(@NotNull String message) {
        this.message = message;
    }

    @Override
    public int getType() {
        return 0;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        StreamUtils.writeString(output, message);
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        message = StreamUtils.readString(input);
    }

    public String getMessage() {
        return message;
    }
}
