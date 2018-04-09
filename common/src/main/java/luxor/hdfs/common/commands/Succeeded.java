package luxor.hdfs.common.commands;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class Succeeded extends Pipeable {
    @Override
    public int getType() {
        return -1;
    }

    @Override
    public void serializeContents(OutputStream output) throws IOException {
        //nothing to do.
    }

    @Override
    public void deserializeContents(InputStream input) throws IOException {
        //nothing to do.
    }

    @Override
    public String toString() {
        return "Succeeded";
    }
}
