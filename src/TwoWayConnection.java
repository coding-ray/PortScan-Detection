import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * NetFlow key used in filter and session extraction stage
 */
public class TwoWayConnection implements WritableComparable<TwoWayConnection> {
  protected LongWritable srcIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  protected IntWritable srcPort; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.
  protected LongWritable dstIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  protected IntWritable dstPort; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.

  // Whitelist IP addresses
  private static List<Long> whitelist = null;

  public TwoWayConnection() {
    srcIP = new LongWritable();
    srcPort = new IntWritable();
    dstIP = new LongWritable();
    dstPort = new IntWritable();
  }

  public TwoWayConnection(long srcIP, int srcPort, long dstIP, int dstPort) {
    this.srcIP = new LongWritable(srcIP);
    this.srcPort = new IntWritable(srcPort);
    this.dstIP = new LongWritable(dstIP);
    this.dstPort = new IntWritable(dstPort);
  }

  public TwoWayConnection(IPPortPair src, IPPortPair dst) {
    this.srcIP = new LongWritable(src.getIPLong());
    this.srcPort = new IntWritable(src.getPort());
    this.dstIP = new LongWritable(dst.getIPLong());
    this.dstPort = new IntWritable(dst.getPort());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    srcIP.readFields(in);
    srcPort.readFields(in);
    dstIP.readFields(in);
    dstPort.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    srcIP.write(out);
    srcPort.write(out);
    dstIP.write(out);
    dstPort.write(out);
  }

  @Override
  public int compareTo(TwoWayConnection other) {
    if (srcIP.compareTo(other.srcIP) != 0)
      return srcIP.compareTo(other.srcIP);

    if (dstIP.compareTo(other.dstIP) != 0)
      return dstIP.compareTo(other.dstIP);

    if (srcPort.compareTo(other.srcPort) != 0)
      return srcPort.compareTo(other.srcPort);

    return dstPort.compareTo(other.dstPort);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TwoWayConnection))
      return false;

    TwoWayConnection o = (TwoWayConnection) other;
    return srcIP.equals(o.srcIP) &&
        srcPort.equals(o.srcPort) &&
        dstIP.equals(o.dstIP) &&
        dstPort.equals(o.dstPort);
  }

  @Override
  public int hashCode() {
    /*
     * srcIP and dstIP are ranged in 0 ~ (2^32 - 1),
     * and srcPort and dstPort are ranged in 0 ~ 2^16.
     * To map the composition of them into the range (-2^31) ~ (2^31 - 1),
     * I multiply them together to have range 0 ~ (2^64 * 2^32) (approximately),
     * divide it by 2^64, and substract it by 2^31.
     */
    double hash = (double) srcIP.get() * dstIP.get()
        * srcPort.get() * dstPort.get()
        / Long.MAX_VALUE
        + Integer.MIN_VALUE;

    return (int) hash;
  }

  public static void initWhitelist()
      throws IllegalArgumentException, IOException {
    whitelist = NFWhitelistInitializer.getWhitelist();
  }

  public boolean isInWhitelist() {
    for (long ip : whitelist) {
      if (ip == srcIP.get() || ip == dstIP.get())
        return true;
    }
    return false;
  }

  // Get the source and destination IPs and ports
  @Override
  public String toString() {
    return convertIPToString(srcIP.get()) + ":" + srcPort.toString() +
        "\t<->\t" +
        convertIPToString(dstIP.get()) + ":" + dstPort.toString();
  }

  public static String convertIPToString(long ip /* IPv4 */) {
    return ((ip & 0xFF000000) >> 24) + "." +
        ((ip & 0x00FF0000) >> 16) + "." +
        ((ip & 0x0000FF00) >> 8) + "." +
        (ip & 0x000000FF);
  }
}

