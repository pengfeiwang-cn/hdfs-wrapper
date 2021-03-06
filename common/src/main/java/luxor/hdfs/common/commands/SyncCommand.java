package luxor.hdfs.common.commands;

public class SyncCommand extends FlushCommand {
    @Override
    public int getType() {
        return Pipeable.SYNC;
    }

    public SyncCommand() {}

    public SyncCommand(String namedPipe) {
        super(namedPipe);
    }

    @Override
    public String toString() {
        return String.format("SyncCommand:{namedPipe='%s'}", getNamedPipe());
    }
}
