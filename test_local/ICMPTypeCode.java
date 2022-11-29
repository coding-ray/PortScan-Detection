package test_local;

import java.util.regex.Pattern;

public class ICMPTypeCode {
  public byte type; // 0: echo reply, 3: destination unreachable, 8: echo request
  public byte code;
  /*
   * (type 0) 0: echo reply
   * (type 3) 1: destination host unreachable
   * (type 3) 3: destination port unreachable
   * (type 3) 10: host administratively prohibited
   * (type 8) 0: echo request
   */

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

  static private final Pattern DOT_PATTERN = Pattern.compile("\\.");

  public ICMPTypeCode(String NFPort) {
    // NFPort format: type.code
    // Exapmle: 0.0

    String[] type_and_code = DOT_PATTERN.split(NFPort);
    type = Byte.parseByte(type_and_code[0]);
    code = Byte.parseByte(type_and_code[1]);
  }
}
