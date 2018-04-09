package luxor.hdfs.common.commands;

public class CloseWriterCommand extends FlushCommand {
    public CloseWriterCommand() {}

    public CloseWriterCommand(String namedPipe) {
        super(namedPipe);
    }

    @Override
    public int getType() {
        return Pipeable.CLOSEWRITER;
    }

    @Override
    public String toString() {
        return String.format("CloseWriterCommand:{namedpipe='%s'}", getNamedPipe());
    }
}
