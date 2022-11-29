import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * NetFlow key used in filter and session extraction stage
 */
public class NFKey implements WritableComparable<NFKey> {
  private LongWritable srcIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  private IntWritable srcPort; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.
  private LongWritable dstIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  private IntWritable dstPort; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.

  public NFKey() {
    srcIP = new LongWritable();
    srcPort = new IntWritable();
    dstIP = new LongWritable();
    dstPort = new IntWritable();
  }

  public NFKey(long srcIP, int srcPort, long dstIP, int dstPort) {
    this.srcIP = new LongWritable(srcIP);
    this.srcPort = new IntWritable(srcPort);
    this.dstIP = new LongWritable(dstIP);
    this.dstPort = new IntWritable(dstPort);
  }

  public void set(LongWritable srcIP, IntWritable srcPort, LongWritable dstIP,
      IntWritable dstPort) {
    this.srcIP = srcIP;
    this.srcPort = srcPort;
    this.dstIP = dstIP;
    this.dstPort = dstPort;
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
  public int compareTo(NFKey other) {
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
    if (!(other instanceof NFKey))
      return false;

    NFKey o = (NFKey) other;
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

  public boolean isInWhiteList() {
    return false;
  }

  // Get the source and destination IPs and ports
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