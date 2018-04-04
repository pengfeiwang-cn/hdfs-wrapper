import luxor.hdfs.fs.PipedFileSystem;
import org.apache.log4j.Logger;

public class ChildProc {

    private static final Logger logger = Logger.getLogger(ChildProc.class);
    /*
     * 测试用例实现Runnable,通过ChildProc来加载.
     */
    public static void main(String[] args) {
        try {
            PipedFileSystem.setLocalWorkingDir("/tmp/test/");
            Class r = Class.forName(args[0]);
            Runnable runnable = (Runnable)r.newInstance();
            runnable.run();
        } catch (ClassNotFoundException e) {
            logger.error("ChildProc", e);
            System.exit(-1);
        } catch (InstantiationException e) {
            logger.error("ChildProc", e);
            System.exit(-1);
        } catch (IllegalAccessException e) {
            logger.error("ChildProc", e);
            System.exit(-1);
        } catch (Exception e) {
            logger.error("ChildProc", e);
            System.exit(-1);
        }

    }
}
