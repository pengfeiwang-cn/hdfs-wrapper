package luxor.hdfs.common.commands;

import luxor.hdfs.common.StreamUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class Pipeable {
    public static final int SUCCEED = -1;
    public static final int EXCEPTION = 0;
    public static final int OPEN = 1;
    public static final int CREATE = 2;
    public static final int READ = 3;
    public static final int SEEK = 4;
    public static final int GETPOSITION = 5;
    public static final int SEEKINNEWSOURCE = 6;
    public static final int WRITE = 7;
    public static final int FLUSH = 8;
    public static final int CLOSEREADER = 9;
    public static final int CLOSEWRITER = 10;
    public static final int SYNC = 11;
    public static final int LISTSTATUS = 12;
    public static final int DELETE = 13;
    public static final int RENAME = 14;
    public static final int APPEND = 15;
    public static final int SETWORKINGDIRECTORY = 16;
    public static final int GETWORKINGDIRECTORY = 17;
    public static final int MKDIR = 18;
    public static final int GETFILESTATUS = 19;

    public abstract int getType();
    public abstract void serializeContents(OutputStream output) throws IOException;
    public abstract void deserializeContents(InputStream input) throws IOException;

    private static final Logger logger = Logger.getLogger(Pipeable.class);

    public static void serialize(Pipeable cmd, OutputStream output) throws IOException {
        StreamUtils.writeInt(output, cmd.getType());
        cmd.serializeContents(output);
        output.flush();
    }

    // A factory method.
    public static Pipeable deserialize(InputStream input) throws IOException {
        Pipeable ret = null;

        int type = StreamUtils.readInt(input);
        switch (type) {
            case SUCCEED:
                ret = new Succeeded();
                break;
            case EXCEPTION:
                ret = new PipedException();
                break;
            case OPEN:
                ret = new OpenCommand();
                break;
            case CREATE:
                ret = new CreateCommand();
                break;
            case READ:
                ret = new ReadCommand();
                break;
            case SEEK:
                ret = new SeekCommand();
                break;
            case GETPOSITION:
                ret = new GetPositionCommand();
                break;
            case SEEKINNEWSOURCE:
                ret = new SeekInNewSourceCommand();
                break;
            case WRITE:
                ret = new WriteCommand();
                break;
            case FLUSH:
                ret = new FlushCommand();
                break;
            case CLOSEREADER:
                ret = new CloseReaderCommand();
                break;
            case CLOSEWRITER:
                ret = new CloseWriterCommand();
                break;
            case SYNC:
                ret = new SyncCommand();
                break;
            case LISTSTATUS:
                ret = new ListStatusCommand();
                break;
            case DELETE:
                ret = new DeleteCommand();
                break;
            case RENAME:
                ret = new RenameCommand();
                break;
            case APPEND:
                ret = new AppendCommand();
                break;
            case SETWORKINGDIRECTORY:
                ret = new SetWorkingDirectoryCommand();
                break;
            case GETWORKINGDIRECTORY:
                ret = new GetWorkingDirectoryCommand();
                break;
            case MKDIR:
                ret = new MkDirCommand();
                break;
            case GETFILESTATUS:
                ret = new GetFileStatusCommand();
                break;
            default:
                assert(false);
        }
        ret.deserializeContents(input);

        return ret;
    }

    protected void logStack() {
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20 && i < stack.length; i++) {
            sb.append('\t');
            sb.append(stack[i]);
            sb.append(System.lineSeparator());
        }
        logger.info(sb.toString());
    }
}
