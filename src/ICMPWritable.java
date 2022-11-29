import java.util.regex.Pattern;
import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ICMPWritable implements Writable {
  /*
   * 0: echo reply
   * 3: destination unreachable
   * 8: echo request
   */
  private ByteWritable type;

  /*
   * (type 0) 0: echo reply
   * (type 3) 1: destination host unreachable
   * (type 3) 3: destination port unreachable
   * (type 3) 10: host administratively prohibited
   * (type 8) 0: echo request
   */
  private ByteWritable code;

  // ICMP type
  public static final byte ECHO_REPLY_TYPE = 0;
  public static final byte UNREACHABLE_TYPE = 3;
  public static final byte ECHO_REQUEST_TYPE = 8;

  // ICMP codes
  public static final byte ECHO_REPLY_CODE = 0;
  public static final byte HOST_UNREACHABLE_CODE = 1;
  public static final byte PORT_UNREACHABLE_CODE = 3;
  public static final byte PROHIBITED_CODE = 10;
  public static final byte ECHO_REQUEST_CODE = 0;

  // Pattern of the ICMP type and code string in NetFlow
  private static final Pattern DOT_PATTERN = Pattern.compile("\\.");

  public ICMPWritable() {
    type = new ByteWritable();
    code = new ByteWritable();
  }

  public ICMPWritable(String icmpTypeCodeString) {
    // Input format: <type>.<code>
    // Exapmle: 0.0

    String[] typeCode = DOT_PATTERN.split(icmpTypeCodeString);
    type = new ByteWritable(Byte.parseByte(typeCode[0]));
    code = new ByteWritable(Byte.parseByte(typeCode[1]));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type.readFields(in);
    code.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    type.write(out);
    code.write(out);
  }
}
