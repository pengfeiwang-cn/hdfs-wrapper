package luxor.hdfs.parent;

import com.sun.istack.internal.NotNull;
import luxor.hdfs.common.ControlChannel;
import luxor.hdfs.common.StreamUtils;
import luxor.hdfs.common.commands.*;
import luxor.hdfs.parent.proxies.InProxy;
import luxor.hdfs.parent.proxies.OutProxy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Driver {
    private ControlChannel controlChannel;
    private Process child;
    private static Logger logger = Logger.getLogger(Driver.class);
    private Map<String, InProxy> readers = new HashMap<String, InProxy>();
    private Map<String, OutProxy> writers = new HashMap<String, OutProxy>();
    private FileSystem fs;
    private int finishedStatus = 0;

    public Driver(@NotNull String cmd) throws IOException {
//        child = Runtime.getRuntime().exec(cmd);
        ProcessBuilder pb = new ProcessBuilder(cmd);
        //pb.directory(new File("/tmp/fuck"));
        File log = new File("/tmp/fuck/log");
        pb.redirectErrorStream(true);
        pb.redirectOutput(ProcessBuilder.Redirect.appendTo(log));
        child = pb.start();
        try {
            int fuck = child.waitFor();
            System.out.println(fuck);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        controlChannel = new ControlChannel(child.getInputStream(), child.getOutputStream());
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    // 0 means OK, others means error.
    public int Run() throws IOException, InterruptedException {
        Thread worker = new Thread() {
            @Override
            public void run() {
                while (true) {
                    Pipeable cmd = controlChannel.waitCommand();
                    if (cmd == null) {
                        break;
                    }
                    else {
                        try {
                            switch (cmd.getType()) {
                                case Pipeable.CREATE: {
                                    onCreate((CreateCommand) cmd);
                                    break;
                                }
                                case Pipeable.OPEN: {
                                    onOpen((OpenCommand) cmd);
                                    break;
                                }
                                case Pipeable.READ: {
                                    onRead((ReadCommand) cmd);
                                    break;
                                }
                                case Pipeable.WRITE: {
                                    onWrite((WriteCommand) cmd);
                                    break;
                                }
                                case Pipeable.CLOSEREADER: {
                                    onCloseReader((CloseReaderCommand) cmd);
                                    break;
                                }
                                case Pipeable.CLOSEWRITER: {
                                    onCloseWriter((CloseWriterCommand) cmd);
                                    break;
                                }
                                case Pipeable.FLUSH: {
                                    onFlush((FlushCommand) cmd);
                                    break;
                                }
                                case Pipeable.SYNC: {
                                    onSync((SyncCommand) cmd);
                                    break;
                                }
                                case Pipeable.DELETE: {
                                    onDelete((DeleteCommand) cmd);
                                    break;
                                }
                                case Pipeable.RENAME: {
                                    onRename((RenameCommand) cmd);
                                    break;
                                }
                                case Pipeable.GETFILESTATUS: {
                                    onGetFileStatus((GetFileStatusCommand) cmd);
                                    break;
                                }
                                // TODO: not finished.
                                default:
                                    assert(false);
                            }
                            controlChannel.sendCommand(new Succeeded());
                        }
                        catch (IOException e) {
                            try {
                                controlChannel.sendCommand(new PipedException(e.getMessage()));
                            } catch (IOException ex) {
                                logger.error("Control channel is broken.", ex);
                                finishedStatus = -1;
                            }
                        }
                    }
                }
            }
        };

        worker.start();
        int ret = child.waitFor();
        return (ret != 0 || finishedStatus != 0) ? -1 : 0;
    }

    private void onGetFileStatus(GetFileStatusCommand cmd) throws IOException {
        FileStatus status = fs.getFileStatus(cmd.getPath());
        StreamUtils.writeWritable(controlChannel.getOutput(), status);
    }

    private void onRename(RenameCommand cmd) throws IOException {
        boolean result = fs.rename(cmd.getSrc(), cmd.getDst());
        StreamUtils.writeBoolean(controlChannel.getOutput(), result);
    }

    private void onDelete(DeleteCommand cmd) throws IOException {
        boolean result = fs.delete(cmd.getPath(), cmd.isRecursive());
        StreamUtils.writeBoolean(controlChannel.getOutput(), result);
    }

    private void onCloseReader(CloseReaderCommand cmd) throws IOException {
        assert(readers.containsKey(cmd.getNamedPipe()));
        InProxy inProxy = readers.get(cmd.getNamedPipe());
        inProxy.close();
    }

    private void onCloseWriter(CloseWriterCommand cmd) throws IOException {
        assert(writers.containsKey(cmd.getNamedPipe()));
        OutProxy outProxy = writers.get(cmd.getNamedPipe());
        outProxy.close();
    }

    private void onFlush(FlushCommand cmd) throws IOException {
        assert(writers.containsKey(cmd.getNamedPipe()));
        OutProxy outProxy = writers.get(cmd.getNamedPipe());
        outProxy.hflush();
    }

    private void onSync(SyncCommand cmd) throws IOException {
        assert(writers.containsKey(cmd.getNamedPipe()));
        OutProxy outProxy = writers.get(cmd.getNamedPipe());
        outProxy.hsync();
    }

    private void onRead(ReadCommand rc) throws IOException {
        assert(readers.containsKey(rc.getNamedPipe()));
        InProxy inProxy = readers.get(rc.getNamedPipe());
        inProxy.read(rc.getLength());
    }

    private void onOpen(OpenCommand oc) throws IOException {
        FSDataInputStream input =
                fs.open(new org.apache.hadoop.fs.Path(oc.getPath()), oc.getBufferSize());
        FileOutputStream dataChannel = new FileOutputStream(oc.getNamedPipe());
        InProxy inProxy = new InProxy(input, dataChannel);
        readers.put(oc.getNamedPipe(), inProxy);
    }

    private void onCreate(CreateCommand cc) throws IOException {
        FSDataOutputStream output =
                fs.create(new org.apache.hadoop.fs.Path(cc.getPath()),
                        cc.isOverwrite(),
                        cc.getBufferSize(),
                        cc.getReplication(),
                        cc.getBlockSize());
        FileInputStream dataChannel = new FileInputStream(cc.getNamedPipe());
        OutProxy outProxy = new OutProxy(output, dataChannel);
        writers.put(cc.getNamedPipe(), outProxy);
    }

    private void onWrite(WriteCommand wc) throws IOException {
        assert(writers.containsKey(wc.getNamedPipe()));
        OutProxy outProxy = writers.get(wc.getNamedPipe());
        outProxy.write(wc.getLength());
    }
}
