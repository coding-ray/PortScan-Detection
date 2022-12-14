package psd.com;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

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
    // time is set to 0 to show that there is no record there.
    time = new LongWritable(0);
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
    else
      this.icmp = new ICMPWritable(); // since the protocol is not ICMP
  }

  public NFValue(NFValue input) {
    this.time = new LongWritable(input.time.get());
    this.duration = new IntWritable(input.duration.get());
    this.protocol = new IntWritable(input.protocol.get());
    this.packetNumber = new IntWritable(input.packetNumber.get());
    this.packetSize = new LongWritable(input.packetSize.get());
    this.tos = new IntWritable(input.tos.get());
    this.flow = new IntWritable(input.flow.get());
    this.isReversed = new BooleanWritable(input.isReversed.get());
    this.flag = new FlagWritable(input.flag);
    this.icmp = new ICMPWritable(input.icmp);
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

  /**
   * Add duration, packetNumber and flow together, and
   * replace flag with latest flag.
   * 
   * @param input NFValue to be added
   */
  public void combine(NFValue input) {
    duration.set((int) (input.time.get() - time.get())
        + input.duration.get());

    packetNumber.set(packetNumber.get() + input.packetNumber.get());
    packetSize.set(packetSize.get() + input.packetSize.get());
    flow.set(flow.get() + input.flow.get());
    flag.overlapWith(input.flag);
  }

  public FlagWritable getFlag() {
    return flag;
  }

  public long getTime() {
    return time.get();
  }

  public int getDuration() {
    return duration.get();
  }

  public int getProtocol() {
    return protocol.get();
  }

  public boolean getIsReversed() {
    return isReversed.get();
  }

  @Override
  public String toString() {
    if (!this.hasValue())
      return "none";
    else
      return new Timestamp(time.get()) + "\t" +
          duration.toString() + "\t" +
          protocol.toString() + "\t" +
          packetNumber.toString() + "\t" +
          packetSize.toString() + "\t" +
          // tos.toString() + "\t" +
          flow.toString() + "\t" +
          flag.toString();
    // + "\t" + icmp.toString()
  }

  public boolean hasValue() {
    return time.get() != 0;
  }
}

