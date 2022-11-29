package test_local;

import java.util.Scanner;
import java.util.regex.Pattern;
import java.sql.Timestamp;

public class NF {
  public long time; // the time that the flow starts in milliseconds
  public int duration; // in milliseconds
  public int protocol; // 1: ICMP, 2: IGMP, 6: TCP, 17: UDP
  public long srcIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  public int srcPort; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.
  public long dstIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  public int dstPort; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.
  public int packetNumber; // packets, total packet number
  public long packetSize; // bytes, total packet size in bytes
  public int tos; // type of service
  public int flow; // flows. Always be 1 for the raw NetFlow.
  public boolean isReversed; // if true, it means dst -> src

  // Flags
  public Flag flag;

  // ICMP
  public ICMPTypeCode icmp;

  // Protocol numbers
  public static final byte ICMP = 1;
  public static final byte IGMP = 2;
  public static final byte TCP = 6;
  public static final byte UDP = 17;

  static private final Pattern DOT_PATTERN = Pattern.compile("\\.");
  static private final Pattern COLON_PATTERN = Pattern.compile(":");

  // static private final DateTimeFormatter DTF =
  // DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

  public NF(String data /* one line of NetFlow data from nf file */) {
    Scanner s = new Scanner(data);

    // Set time, duration and protocol
    time = Timestamp.valueOf(s.next() + " " + s.next()).getTime();
    duration = Math.round(Float.parseFloat(s.next()) * 1000);
    protocol = Integer.parseInt(s.next());

    // Set source IP and port
    String[] src_ip_and_port = COLON_PATTERN.split(s.next());
    srcIP = convert_ip_to_number(src_ip_and_port[0], protocol);
    srcPort = Integer.parseInt(src_ip_and_port[1]);

    // skip "->"
    s.next();

    // Set destination IP and port
    String[] dst_ip_and_port = COLON_PATTERN.split(s.next());
    dstIP = convert_ip_to_number(dst_ip_and_port[0], protocol);
    if (protocol == TCP || protocol == UDP) {
      dstPort = Integer.parseInt(dst_ip_and_port[1]);
    } else {
      dstPort = 0;
      if (protocol == ICMP) {
        icmp = new ICMPTypeCode(dst_ip_and_port[1]);
      }
    }

    // Make srcIP:srcPort >= dstIP:desPort
    if (srcIP < dstIP || (srcIP == dstIP && srcPort < dstPort)) {
      swapSrcDst();
      isReversed = true;
    } else {
      isReversed = false;
    }

    // Set the rest of attributes
    flag = new Flag(s.next());
    tos = Integer.parseInt(s.next());
    packetNumber = Integer.parseInt(s.next());
    packetSize = Long.parseLong(s.next());
    flow = Integer.parseInt(s.next());

    // End of setup
    s.close();
  }

  public void printData() {
    System.out.println("date (flow start)      : " + new Timestamp(time));
    System.out.println("duration (in ms )      : " + duration);
    System.out.println("protocol               : " +
        ((protocol == TCP) ? "TCP" : (protocol == UDP) ? "UDP" : (protocol == ICMP) ? "ICMP" : protocol));
    System.out.println("srcIP:srcPort          : " +
        convert_number_to_ip(srcIP) + ":" + srcPort);
    System.out.println("dstIP:dstPort          : " +
        convert_number_to_ip(dstIP) + ":" + dstPort);
    System.out.println("flags                  : " +
        (flag.URG ? "U" : ".") +
        (flag.ACK ? "A" : ".") +
        (flag.PSH ? "P" : ".") +
        (flag.RST ? "R" : ".") +
        (flag.SYN ? "S" : ".") +
        (flag.FIN ? "F" : "."));
    System.out.println("packets (packet number): " + packetNumber);
    System.out.println("bytes (packet bytes)   : " + packetSize);
    System.out.println("tos                    : " + tos);
    System.out.println("flows                  : " + flow);
    System.out.println("------------------------------------------------");
  }

  public static long convert_ip_to_number(String ip, int protocol) {
    String[] digits = DOT_PATTERN.split(ip);
    if (!(protocol == TCP || protocol == UDP) ||
        digits.length != 4 /* not IPv4 */)
      return 0;

    return (Long.parseLong(digits[0]) << 24) +
        (Long.parseLong(digits[1]) << 16) +
        (Long.parseLong(digits[2]) << 8) +
        (Long.parseLong(digits[3]));
  }

  public static String convert_number_to_ip(long ip /* IPv4 */) {
    return String.valueOf((ip & 0xFF000000) >> 24) + "." +
        String.valueOf((ip & 0x00FF0000) >> 16) + "." +
        String.valueOf((ip & 0x0000FF00) >> 8) + "." +
        String.valueOf(ip & 0x000000FF);
  }

  private void swapSrcDst() {
    long tempLong = dstIP;
    dstIP = srcIP;
    srcIP = tempLong;

    int tempInt = dstPort;
    dstPort = srcPort;
    srcPort = tempInt;
  }
}