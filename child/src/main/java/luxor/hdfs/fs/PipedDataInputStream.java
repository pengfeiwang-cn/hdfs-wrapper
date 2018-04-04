package luxor.hdfs.fs;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.ControlChannel;
import luxor.hdfs.common.commands.CloseReaderCommand;
import luxor.hdfs.common.commands.Pipeable;
import luxor.hdfs.common.commands.ReadCommand;
import luxor.hdfs.common.commands.SeekCommand;
import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/*
 * 为了避免每次调用InputStream.read()都向控制信道发read指令, 需要成批的读, 要用BufferedInputStream.
 * Java不支持多继承, 而我们的逻辑需要在底层控制向控制信道发送read请求来驱动父进程读真实数据,所以在这里猥琐一下:
 * 1. PipedDataInputStream内部实现个InputStream的派生类InternalInputStream
 * 2. InternalInputStream.ControlledInputStream重载public int read(byte b[], int off, int len)方法
 *      a. 发ReadCommand到到控制信道
 *      b. 读underline input.
 * 之所以只重载read(byte b[], int off, int len)方法,原因是BufferedInputStream的fill方法只调用这个方法获取批量数据.
 *
 * 这个实现很hack, JDK版本升级要确认是否OK. 如果担心兼容性问题, copy BufferedInputStream实现过来.
 */
public class PipedDataInputStream extends FSDataInputStream {
    private final ControlChannel controlChannel;
    private String namedPipe;
    private InternalInputStream input;

    public PipedDataInputStream(@NotNull String namedPipe,
                                @NotNull InputStream in,
                                @NotNull ControlChannel controlChannel) {
        super(new InternalInputStream(in, controlChannel, namedPipe));
        this.input = (InternalInputStream)super.in;
        this.controlChannel = controlChannel;
        this.namedPipe = namedPipe;
    }

    @Override
    public void seek(long desired) throws IOException {
        input.seek(desired);
    }

    @Override
    public long getPos() throws IOException {
        return input.getPosition();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        throw new IOException("seekToNewSource is not supported.");
    }

    @Override
    public void close() throws IOException {
        Pipeable close = new CloseReaderCommand(namedPipe);
        controlChannel.sendCommand(close);
        super.close();
    }

    private static class InternalInputStream extends BufferedInputStream {
        private final ControlChannel controlChannel;
        private String namedPipe;
        private long position = 0;

        public InternalInputStream(InputStream input, ControlChannel controlChannel, String namedPipe) {
            super(new ControlledInputStream(input, controlChannel, namedPipe));
            this.controlChannel = controlChannel;
            this.namedPipe = namedPipe;
        }

        @Override
        public synchronized int read() throws IOException {
            int ret = super.read();
            synchronized (this) {
                position++;
            }

            return ret;
        }

        private static class ControlledInputStream extends InputStream{
            private InputStream input;
            private final ControlChannel controlChannel;
            private String namedPipe;

            public ControlledInputStream(InputStream input, ControlChannel controlChannel, String namedPipe) {
                this.input = input;
                this.controlChannel = controlChannel;
                this.namedPipe = namedPipe;
            }

            @Override
            public int read() throws IOException {
                return input.read();
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                Pipeable read = new ReadCommand(namedPipe, len);
                controlChannel.sendCommand(read);
                return input.read(b, off, len);
            }

            @Override
            public void close() throws IOException {
                input.close();
                super.close();
            }
        }

        public long getPosition() {
            return position;
        }

        public void seek(long desired) throws IOException {
            int left = count - pos;
            long wanted = desired - position;

            if (wanted >= 0) {
                if (wanted <= left) {
                    pos = (int) wanted;
                } else {
                    Pipeable seek = new SeekCommand(namedPipe, wanted - left);
                    controlChannel.sendCommand(seek);
                    pos = count;
                }
            }
            else {
                if (-wanted < pos) {
                    pos = (int)(-wanted);
                }
                else {
                    Pipeable seek = new SeekCommand(namedPipe, desired);
                    controlChannel.sendCommand(seek);
                    pos = count;
                }
            }

            position = desired;
        }
    }
}
