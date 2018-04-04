import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class Program {

    public static void main(String[] args) {
        String s = "我了个擦";
        try {
            FileOutputStream fs = new FileOutputStream("/tmp/fuck");
            DataOutputStream ds = new DataOutputStream(fs);
            ds.writeBytes(s);
            ds.flush();
            ds.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
