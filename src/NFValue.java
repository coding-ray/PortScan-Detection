import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class NFValue implements Writable {
  private LongWritable time; // the time that the flow starts in milliseconds
  private IntWritable duration; // in milliseconds
  private IntWritable protocol; // 1: ICMP, 2: IGMP, 6: TCP, 17: UDP
  private IntWritable packetNumber; // packets, total packet number
  private LongWritable packetSize; // bytes, total packet size in bytes
  private IntWritable tos; // type of service
  private IntWritable flow; // flows. Always be 1 for the raw NetFlow.
  private BooleanWritable isReversed; // if true, it means dst -> src

  // Flags
  private FlagWritable flag;

  // ICMP
  private ICMPWritable icmp;

  public NFValue() {
    time = new LongWritable();
    duration = new IntWritable();
    protocol = new IntWritable();
    packetNumber = new IntWritable();
    packetSize = new LongWritable();
    tos = new IntWritable();
    flow = new IntWritable();
    isReversed = new BooleanWritable();
    flag = new FlagWritable();
    icmp = new ICMPWritable();
  }

  public NFValue(long time, int duration, int protocol, int packetNumber,
      long packetSize, int tos, int flow, boolean isReversed, String flagString,
      String icmpString) {
    this.time = new LongWritable(time);
    this.duration = new IntWritable(duration);
    this.protocol = new IntWritable(protocol);
    this.packetNumber = new IntWritable(packetNumber);
    this.packetSize = new LongWritable(packetSize);
    this.tos = new IntWritable(tos);
    this.flow = new IntWritable(flow);
    this.isReversed = new BooleanWritable(isReversed);
    this.flag = new FlagWritable(flagString);
    if (icmpString.length() > 0)
      this.icmp = new ICMPWritable(icmpString);
  }

  public void set(LongWritable time, IntWritable duration, IntWritable protocol,
      IntWritable packetNumber, LongWritable packetSize, IntWritable tos,
      IntWritable flow, BooleanWritable isReversed, FlagWritable flag,
      ICMPWritable icmp) {
    this.time = time;
    this.duration = duration;
    this.protocol = protocol;
    this.packetNumber = packetNumber;
    this.packetSize = packetSize;
    this.tos = tos;
    this.flow = flow;
    this.isReversed = isReversed;
    this.flag = flag;
    this.icmp = icmp;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    time.readFields(in);
    duration.readFields(in);
    protocol.readFields(in);
    packetNumber.readFields(in);
    packetSize.readFields(in);
    tos.readFields(in);
    flow.readFields(in);
    isReversed.readFields(in);
    flag.readFields(in);
    icmp.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    time.write(out);
    duration.write(out);
    protocol.write(out);
    packetNumber.write(out);
    packetSize.write(out);
    tos.write(out);
    flow.write(out);
    isReversed.write(out);
    flag.write(out);
    icmp.write(out);
  }

  public String toString() {
    return new String("");
    // todo
  }
}
