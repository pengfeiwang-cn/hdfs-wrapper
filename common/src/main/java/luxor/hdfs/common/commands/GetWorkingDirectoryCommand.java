package luxor.hdfs.common.commands;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GetWorkingDirectoryCommand extends Pipeable {
    @Override
    public int getType() {
        return Pipeable.GETWORKINGDIRECTORY;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {

    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {

    }

    @Override
    public String toString() {
        return "GetWorkingDirectoryCommand";
    }
}
