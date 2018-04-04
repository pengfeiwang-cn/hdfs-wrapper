package luxor.hdfs.common.commands;

public class CloseReaderCommand extends FlushCommand {
    public CloseReaderCommand() {}

    public CloseReaderCommand(String namedPipe) {
        super(namedPipe);
    }

    @Override
    public int getType() {
        return Pipeable.CLOSEREADER;
    }
}
