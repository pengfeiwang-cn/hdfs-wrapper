package luxor.hdfs.common.commands;

public class SeekInNewSourceCommand extends SeekCommand {

    public SeekInNewSourceCommand() {}

    public SeekInNewSourceCommand(String namedPipe, long desired) {
        super(namedPipe, desired);
    }

    @Override
    public int getType() {
        return Pipeable.SEEKINNEWSOURCE;
    }
}
