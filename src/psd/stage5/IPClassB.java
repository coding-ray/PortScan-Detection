package psd.stage5;

import java.util.regex.Pattern;

public class IPClassB {
  private long ip; // 1.2/16 = 1<<24 + 2<<16

  private static final Pattern SUBNET_PATTERN = Pattern.compile("[./]");
  public static final long SUBNET_MASK = 0xFFFF0000;

  public IPClassB() {
    ip = 0;
  }

  /*
   * Sample input: 192.168/16
   */
  public IPClassB(String ipWithSubnet) {
    String[] elements = SUBNET_PATTERN.split(ipWithSubnet);
    if (elements.length <= 2)
      ip = 0;
    else if (elements.length == 3) {
      ip = ((long) Integer.parseInt(elements[0]) << 24) +
          (Integer.parseInt(elements[1]) << 16);
    } else if (elements.length == 4) {
      ip = ((long) Integer.parseInt(elements[0]) << 24) +
          (Integer.parseInt(elements[1]) << 16) +
          (Integer.parseInt(elements[2]) << 8) +
          (Integer.parseInt(elements[3]));
    }
  }

  public long getIP() {
    return ip;
  }

  public static String toString(long inputIP) {
    return ((inputIP & 0xFF000000) >> 24) + "." +
        ((inputIP & 0x00FF0000) >> 16) + "/16";
  }

  @Override
  public String toString() {
    return IPClassB.toString(ip);
  }
}

