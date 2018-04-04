package luxor.hdfs.common;

import com.sun.istack.internal.NotNull;
import org.apache.hadoop.io.Writable;

import java.io.*;

public class StreamUtils {
    public static void writeWritable(OutputStream out, Writable writable) throws IOException {
        DataOutputStream ds = new DataOutputStream(out);
        writable.write(ds);
        ds.flush();
    }

    public static void readWritable(InputStream in, Writable writable) throws IOException {
        DataInputStream ds = new DataInputStream(in);
        writable.readFields(ds);
    }

    public static void writeWritables(OutputStream out, Writable[] writables) throws IOException {
        writeInt(out, writables.length);
        for (Writable item : writables) {
            StreamUtils.writeWritable(out, item);
        }
    }

    public static void writeBoolean(OutputStream out, boolean v) throws IOException {
        out.write(v ? 1 : 0);
    }

    public static boolean readBoolean(InputStream in) throws IOException {
        return in.read() != 0;
    }

    public static void writeShort(OutputStream out, short v) throws IOException {
        out.write(toByteArray(v));
    }

    public static short readShort(InputStream in) throws IOException {
        byte[] b = new byte[2];
        int rl = in.read(b);
        if (rl != 2) {
            throw new IOException(String.format("Read failed when readShort, 2 bytes expected, but read %s.", rl));
        }

        return toShort(b);
    }

    public static void writeInt(OutputStream out, int v) throws IOException {
        out.write(toByteArray(v));
    }

    public static int readInt(InputStream in) throws IOException {
        byte[] b = new byte[4];
        int rl = in.read(b);
        if (rl != 4) {
            throw new IOException(String.format("Read failed when readInt, 4 bytes expected, but read %s.", rl));
        }

        return toInt(b);
    }

    public static void writeLong(OutputStream out, long v) throws IOException {
        out.write(toByteArray(v));
    }

    public static long readLong(InputStream in) throws IOException {
        byte[] b = new byte[8];
        int rl = in.read(b);
        if (rl != 8) {
            throw new IOException(String.format("Read failed when readLong, 8 bytes expected, but read %s.", rl));
        }

        return toLong(b);
    }

    public static void writeString(OutputStream out, String v) throws IOException {
        byte[] b = v.getBytes();
        writeInt(out, b.length);
        out.write(b);
    }

    public static String readString(InputStream in) throws IOException {
        int len = readInt(in);
        byte[] b = new byte[len];
        int rl = in.read(b);
        if (rl != len) {
            throw new IOException(String.format("Read failed when readString, %s bytes expected, but read %s.",
                    len, rl));
        }
        return new String(b);
    }

    // /(ㄒoㄒ)/~~
    public static byte[] toByteArray(long value) {
        byte[] result = new byte[8];

        for(int i = 7; i >= 0; --i) {
            result[i] = (byte)((int)(value & 255L));
            value >>= 8;
        }

        return result;
    }

    public static byte[] toByteArray(int value) {
        return new byte[] {
                (byte)(value >> 24), (byte)(value >> 16), (byte)(value >> 8), (byte)value
        };
    }

    public static byte[] toByteArray(short value) {
        return new byte[] {(byte)(value >> 8), (byte)value};
    }

    public static short toShort(@NotNull byte[] b) {
        assert(b.length == 2);

        return (short)(b[0] << 8 | (b[1] & 0xFF));
    }

    public static int toInt(@NotNull byte[] b) {
        assert(b.length == 4);

        return b[0] << 24
                | (b[1] & 0xFF) << 16
                | (b[2] & 0xFF) << 8
                | (b[3] & 0xFF);
    }

    public static long toLong(byte[] b) {
        assert(b.length == 8);

        return (b[0] & 0xFFL) << 56
                | (b[1] & 0xFFL) << 48
                | (b[2] & 0xFFL) << 40
                | (b[3] & 0xFFL) << 32
                | (b[4] & 0xFFL) << 24
                | (b[5] & 0xFFL) << 16
                | (b[6] & 0xFFL) << 8
                | (b[7] & 0xFFL);
    }
}
