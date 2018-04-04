package luxor.hdfs.fs;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.ControlChannel;
import luxor.hdfs.common.commands.*;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class PipedDataOutputStream extends FSDataOutputStream {
    private InternalOutputStream output;
    private ControlChannel controlChannel;
    private String namedPipe;

    public PipedDataOutputStream(OutputStream out,
                                 FileSystem.Statistics stats,
                                 ControlChannel controlChannel,
                                 String namedPipe) throws IOException {
        this(out, stats, 0, controlChannel, namedPipe);
    }

    public PipedDataOutputStream(@NotNull OutputStream out,
                                 @NotNull FileSystem.Statistics stats,
                                 long startPosition,
                                 @NotNull ControlChannel controlChannel,
                                 @NotNull String namedPipe) throws IOException {
        super(new InternalOutputStream(out, controlChannel, namedPipe, startPosition), stats, startPosition);
        this.output = (InternalOutputStream)super.out;
        this.controlChannel = controlChannel;
        this.namedPipe = namedPipe;
    }

    @Override
    public long getPos() throws IOException {
        return output.getPosition();
    }

    @Override
    public void flush() throws IOException {
        output.flush();
    }

    @Override
    public void hflush() throws IOException {
        output.flush();
        Pipeable flush = new FlushCommand(namedPipe);
        controlChannel.sendCommand(flush);
    }

    @Override
    public void hsync() throws IOException {
        output.flush();
        Pipeable sync = new SyncCommand(namedPipe);
        controlChannel.sendCommand(sync);
    }

    @Override
    public void close() throws IOException {
        Pipeable close = new CloseWriterCommand(namedPipe);
        controlChannel.sendCommand(close);
        super.close();
    }

    /*
     * 和BufferedInputStream类似, BufferedOutputStream只调用了OutputStream的write(byte[], offset, len),
     * 所以只重载这个方法.
     */
    static class InternalOutputStream extends BufferedOutputStream {
        private OutputStream output;
        private long position;

        public InternalOutputStream(OutputStream out,
                                    ControlChannel controlChannel,
                                    String namedPipe,
                                    long position) {
            super(new ControlledOutputStream(out, controlChannel, namedPipe), 8192); // fix the buffer size to 8k.
            this.position = position;
        }

        public long getPosition() {
            return position;
        }

        @Override
        public synchronized void write(int b) throws IOException {
            super.write(b);
            synchronized (this) {
                position++;
            }
        }

        private static class ControlledOutputStream extends OutputStream {
            private OutputStream output;
            private ControlChannel controlChannel;
            private String namedPipe;

            public ControlledOutputStream(OutputStream output, ControlChannel controlChannel, String namedPipe) {
                this.output = output;
                this.controlChannel = controlChannel;
                this.namedPipe = namedPipe;
            }

            @Override
            public void write(int b) throws IOException {
                output.write(b);
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                Pipeable write = new WriteCommand(namedPipe, len);
                controlChannel.sendCommand(write);
                super.write(b, off, len);
                super.flush();
            }

            /*
             * 因为调用write方法时会自动flush, 所以这里的flush什么都不做
             */
            @Override
            public void flush() throws IOException {
            }

            @Override
            public void close() throws IOException {
                output.close();
                super.close();
            }
        }
    }
}
