package luxor.hdfs.common.commands;

public class WriteCommand extends ReadCommand {
    public WriteCommand() {}

    public WriteCommand(String namedPipe, int lenth) {
        super(namedPipe, lenth);
    }

    @Override
    public int getType() {
        return Pipeable.WRITE;
    }

    @Override
    public String toString() {
        return String.format("WriteCommand:{namedPipe='%s', length=%s}", getNamedPipe(), getLength());
    }
}
