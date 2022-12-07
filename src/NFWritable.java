import java.util.Scanner;
import java.util.regex.Pattern;
import java.sql.Timestamp;

public class NFWritable {
  private NFKey key;
  private NFValue value;

  // Protocol numbers
  // Source: https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xml
  public static final byte ICMP = 1;
  public static final byte IGMP = 2;
  public static final byte TCP = 6;
  public static final byte UDP = 17;

  // For ICMP, which has no destination port
  // It is -1 instead of 0 to make hashCode() work
  public static final int NO_PORT = -1;

  // For 0.0.0.0, which means an invalid, unknown or non-applicable target
  public static final long NO_IP = 0;

  // Split patterns for string
  private static final Pattern DOT_PATTERN = Pattern.compile("\\.");
  private static final Pattern COLON_PATTERN = Pattern.compile(":");

  public NFWritable() {
    key = new NFKey();
    value = new NFValue();
  }

  /**
   * Create an Writable containing NetFlow data.
   * 
   * @param data one line of NetFlow data from nf file
   */
  public NFWritable(String data) {
    Scanner s = new Scanner(data);

    // Temporary variables
    long time = 0;
    int duration = 0;
    int protocol = 0;
    long srcIP = 0;
    int srcPort = 0;
    long dstIP = 0;
    int dstPort = 0;
    int packetNumber = 0;
    long packetSize = 0;
    int tos = 0;
    int flow = 0;
    boolean isReversed = false;
    String flagString = new String();
    String icmpString = new String();

    // Set time, duration and protocol
    time = Timestamp.valueOf(s.next() + " " + s.next()).getTime();
    duration = Math.round(Float.parseFloat(s.next()) * 1000);
    protocol = Integer.parseInt(s.next());

    // Set source IP and port
    String[] src = COLON_PATTERN.split(s.next());
    srcIP = convertIPtoNumber(src[0], protocol);
    // There are records come with only IP address (5 or 6 bytes),
    // and they should be ignored.
    if (src.length >= 2)
      srcPort = Integer.parseInt(src[1]);

    // skip "->"
    s.next();

    // Set destination IP and port
    String[] dst = COLON_PATTERN.split(s.next());
    dstIP = convertIPtoNumber(dst[0], protocol);
    if (dst.length >= 2) {
      // There are records come with only IP address (5 or 6 bytes),
      // and they should be ignored.
      if (protocol == TCP || protocol == UDP) {
        dstPort = Integer.parseInt(dst[1]);
      } else {
        dstPort = -1;
        if (protocol == ICMP) {
          icmpString = dst[1];
        }
      }
    }

    // Make srcIP:srcPort >= dstIP:desPort
    if (srcIP < dstIP || (srcIP == dstIP && srcPort < dstPort)) {
      // Swap src and dst
      long tempIP = dstIP;
      dstIP = srcIP;
      srcIP = tempIP;

      int tempPort = dstPort;
      dstPort = srcPort;
      srcPort = tempPort;

      isReversed = true;
    } else {
      isReversed = false;
    }

    // Set the rest of attributes
    flagString = s.next();
    tos = Integer.parseInt(s.next());
    packetNumber = Integer.parseInt(s.next());
    packetSize = Long.parseLong(s.next());
    flow = Integer.parseInt(s.next());

    // End of setup
    key = new NFKey(srcIP, srcPort, dstIP, dstPort);
    value = new NFValue(time, duration, protocol, packetNumber, packetSize,
        tos, flow, isReversed, flagString, icmpString);

    s.close();
  }

  public static long convertIPtoNumber(String ip, int protocol) {
    String[] digits = DOT_PATTERN.split(ip);
    if (!(protocol == TCP || protocol == UDP) ||
        digits.length != 4 /* not IPv4 */)
      return 0;

    return (Long.parseLong(digits[0]) << 24) +
        (Long.parseLong(digits[1]) << 16) +
        (Long.parseLong(digits[2]) << 8) +
        (Long.parseLong(digits[3]));
  }

  public NFKey getKey() {
    return key;
  }

  public NFValue getValue() {
    return value;
  }

  public boolean isInWhitelist() {
    return key.isInWhitelist();
  }
}

