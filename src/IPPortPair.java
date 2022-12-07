import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.io.IntWritable;

public class IPPortPair implements WritableComparable<IPPortPair> {
  private LongWritable ip; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  private IntWritable port; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.

  private static final Pattern COLON_PATTERN = Pattern.compile(":");

  public IPPortPair() {
    ip = new LongWritable();
    port = new IntWritable();
  }

  public IPPortPair(String input) {
    String[] elements = COLON_PATTERN.split(input);
    if (elements.length > 0)
      ip = new LongWritable(
          NFWritable.convertIPtoNumber(elements[0], NFWritable.TCP));
    else
      ip = new LongWritable(0);

    if (elements.length > 1)
      port = new IntWritable(Integer.parseInt(elements[1]));
    else
      port = new IntWritable(0);
  }

  public String getIP() {
    return NFKey.convertIPToString(ip.get());
  }

  public long getIPLong() {
    return ip.get();
  }

  public LongWritable getIPWritable() {
    return ip;
  }

  public int getPort() {
    return port.get();
  }

  public IntWritable getPortWritable() {
    return port;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ip.readFields(in);
    port.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    ip.write(out);
    port.write(out);
  }

  @Override
  public int compareTo(IPPortPair other) {
    if (ip.compareTo(other.ip) != 0)
      return ip.compareTo(other.ip);

    return port.compareTo(other.port);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IPPortPair))
      return false;

    IPPortPair o = (IPPortPair) other;
    return ip.equals(o.ip) && port.equals(o.port);
  }

  @Override
  public int hashCode() {
    /*
     * ip * port is ranged in 0 ~ (2^32 * 2^16) approximately,
     * To map it into the range (-2^31) ~ (2^31 - 1),
     * divide it by 2^16, and substract it by 2^15.
     */
    long hash = ip.get() * port.get()
        / (Short.MIN_VALUE * -2)
        + Short.MIN_VALUE;

    return (int) hash;
  }

  @Override
  public String toString() {
    return NFKey.convertIPToString(ip.get()) + ":" + port.toString();
  }
}

