package luxor.hdfs.fs;

import luxor.hdfs.common.ControlChannel;
import luxor.hdfs.common.NamedPipe;
import luxor.hdfs.common.StreamUtils;
import luxor.hdfs.common.commands.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.Future;


/**
 * 在Luxor中,我们有两种类型的HDFS操作:
 * 1. 用户通过HDFS API直接访问HDFS
 * 2. UQuery自身内置功能, 如:写内部表, 访问HDFS
 * 对场景1, 通过进程安全沙箱的network namespace白名单解决.
 * 对场景2, 通过PipedFileSystem与父进程交换数据, 由父进程代理去写Luxor内部存储.
 *
 * 作为子进程, 控制信道依赖stdin/stdout, 数据信道由open/create/append方法调用时直接创建命名pipe构建.
 *
 * 前期尚无(或可以规避, 比如说要读取的文件EOF)父进程主动通过控制信道发消息给子进程的需求, 控制信道上的通信由
 * 子进程发起, 根据通信类型, 子进程自己决定是否需要等待父进程返回结果. 发送Request到接受Response之间可以认为
 * 是同步过程, 中间不会插入其他消息——代码实现基于此假设, 干掉了的话, 加个Response MSG缓冲 + Request ID解.
 */
public class PipedFileSystem extends FileSystem {
    private static String localWorkingDir = System.getProperty("user.dir");
    private static Logger logger = Logger.getLogger(PipedFileSystem.class);

    public static String getLocalWorkingDir() {
        return localWorkingDir;
    }

    public static void setLocalWorkingDir(String workingDir) {
        PipedFileSystem.localWorkingDir = workingDir;
    }

    private static final ControlChannel controlChannel = new ControlChannel(System.in, System.out);

    @Override
    public URI getUri() {
        return URI.create(luxor.hdfs.common.FileSystem.URI_PREFIX);
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
        String namedPipe = createNamedPipe(f);
        OpenCommand open = new OpenCommand(f.toString(), namedPipe, bufferSize);
        controlChannel.sendRequest(open);
        FileInputStream inputStream = new FileInputStream(namedPipe);

        return new PipedDataInputStream(namedPipe, inputStream, controlChannel);
    }

    private String createNamedPipe(Path f) throws IOException {
        try {
            String namedPipe = String.format("%s/%s.%s", localWorkingDir, UUID.randomUUID().toString(), f.getName());
            logger.info(String.format("Creating namedPipe for '%s' at '%s'.", f, namedPipe));
            NamedPipe.createPipe(namedPipe);
            logger.info(String.format("Creating namedPipe for path '%s' ended.", f));
            return namedPipe;
        }
        catch (IOException e) {
            logger.trace("createNamedPipe", e);
            throw e;
        }
    }

    @Override
    public FSDataOutputStream create(Path f,
                                     FsPermission permission,
                                     boolean overwrite,
                                     int bufferSize,
                                     short replication,
                                     long blockSize,
                                     Progressable progress) throws IOException {
        /*
         * progress参数和permission参数忽略先, 有需要再加.
         */
        String namedPipe = createNamedPipe(f);
        logger.info(String.format("Named pipe '%s' has been created.", namedPipe));
        Future<FileOutputStream> outputStream = NamedPipe.createOutputStreamAsync(namedPipe);
        logger.info(String.format("Named pipe '%s' has been opened.", namedPipe));

        CreateCommand create =
                new CreateCommand(f.toString(), namedPipe, overwrite, bufferSize, replication, blockSize);
        controlChannel.sendRequest(create);

        try {
            return new PipedDataOutputStream(outputStream.get(), null, controlChannel, namedPipe);
        } catch (Exception e) {
            logger.error("Getting result from async createOutputStreamAsync failed.", e);
            throw new IOException("Getting result from async createOutputStreamAsync failed.", e);
        }
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
        String namedPipe = createNamedPipe(f);
        AppendCommand append = new AppendCommand(f, bufferSize);
        controlChannel.sendRequest(append);
        FileOutputStream outputStream = new FileOutputStream(namedPipe);
        return new PipedDataOutputStream(outputStream, null, controlChannel, namedPipe);
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
        Pipeable cmd = new RenameCommand(src, dst);
        synchronized (controlChannel) {
            controlChannel.sendRequest(cmd);
            return StreamUtils.readBoolean(controlChannel.getInput());
        }
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
        Pipeable cmd = new DeleteCommand(f, recursive);
        synchronized (controlChannel) {
            controlChannel.sendRequest(cmd);
            return StreamUtils.readBoolean(controlChannel.getInput());
        }
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
        Pipeable cmd = new ListStatusCommand(f);
        FileStatus[] ret;
        synchronized (controlChannel) {
            controlChannel.sendRequest(cmd);
            int len = StreamUtils.readInt(controlChannel.getInput());
            ret = new FileStatus[len];

            for (int i = 0; i < len; i++) {
                ret[i] = new FileStatus();
                StreamUtils.readWritable(controlChannel.getInput(), ret[i]);
            }
        }

        return ret;
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
        Pipeable set = new SetWorkingDirectoryCommand(new_dir);
        try {
            controlChannel.sendRequest(set);
        }
        catch (IOException e) { // It must be something wrong with control channel, exist -1.
            logger.error(e);
            System.exit(-1);
        }
    }

    @Override
    public Path getWorkingDirectory() {
        Pipeable get = new GetWorkingDirectoryCommand();
        try {
            synchronized (controlChannel) {
                controlChannel.sendRequest(get);
                return new Path(StreamUtils.readString(controlChannel.getInput()));
            }
        }
        catch (IOException e) {
            logger.error(e);
            System.exit(-1);
        }

        return null; // just make IDE happy.
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
        Pipeable cmd = new MkDirCommand(f);
        synchronized (controlChannel) {
            controlChannel.sendRequest(cmd);
            return StreamUtils.readBoolean(controlChannel.getInput());
        }
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
        Pipeable cmd = new GetFileStatusCommand(f);
        synchronized (controlChannel) {
            controlChannel.sendRequest(cmd);
            FileStatus ret = new FileStatus();
            StreamUtils.readWritable(controlChannel.getInput(), ret);
            return ret;
        }
    }
}
