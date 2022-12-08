import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class PortScanVerticalConnection
    implements WritableComparable<PortScanVerticalConnection> {
  private LongWritable srcIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4
  private IntWritable srcPort; // 1 ~ 65535 for TCP or UDP. 0 for other protocols.
  private LongWritable dstIP; // 1.2.3.4 = 1<<24 + 2<<16 + 3<<8 + 4

  public PortScanVerticalConnection() {
    srcIP = new LongWritable(0);
    srcPort = new IntWritable(0);
    dstIP = new LongWritable(0);
  }

  public PortScanVerticalConnection(IPPortPair src, IPPortPair dst) {
    srcIP = src.getIPWritable();
    srcPort = src.getPortWritable();
    dstIP = dst.getIPWritable();
  }

  public PortScanVerticalConnection(String srcString, String dstString) {
    IPPortPair src = new IPPortPair(srcString);
    IPPortPair dst = new IPPortPair(dstString);
    srcIP = src.getIPWritable();
    srcPort = src.getPortWritable();
    dstIP = dst.getIPWritable();
  }

  public boolean hasValue() {
    return (srcIP.get() != 0) || (dstIP.get() != 0);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    srcIP.readFields(in);
    srcPort.readFields(in);
    dstIP.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    srcIP.write(out);
    srcPort.write(out);
    dstIP.write(out);
  }

  @Override
  public int compareTo(PortScanVerticalConnection other) {
    if (srcIP.compareTo(other.srcIP) != 0)
      return srcIP.compareTo(other.srcIP);

    if (dstIP.compareTo(other.dstIP) != 0)
      return dstIP.compareTo(other.dstIP);

    return srcPort.compareTo(other.srcPort);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PortScanVerticalConnection))
      return false;

    PortScanVerticalConnection o = (PortScanVerticalConnection) other;
    return srcIP.equals(o.srcIP) &&
        srcPort.equals(o.srcPort) &&
        dstIP.equals(o.dstIP);
  }

  @Override
  public int hashCode() {
    long hash = srcIP.get() * srcPort.get() / Integer.MIN_VALUE
        * dstIP.get()
        + Integer.MIN_VALUE;

    return (int) hash;
  }

  @Override
  public String toString() {
    return TwoWayConnection.convertIPToString(srcIP.get()) + ":"
        + srcPort.toString() +
        "\t->\t" +
        TwoWayConnection.convertIPToString(dstIP.get()); // todo: add "V" which denotes vertical
  }

  public Text toText() {
    return new Text(this.toString());
  }
}

