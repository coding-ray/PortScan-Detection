package psd.stage3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import psd.com.FlagWritable;
import psd.com.NFWritable;

public class NFFeature implements Writable {
  // Basic features
  protected LongWritable timeStart; // the time that the host starts to send data in milliseconds
  protected LongWritable timeEnd; // the time that the host finish sending data in milliseconds
  protected IntWritable duration; // in milliseconds
  protected IntWritable protocol; // 1: ICMP, 2: IGMP, 6: TCP, 17: UDP
  protected IntWritable packetNumber; // packets, total packet number
  protected LongWritable packetSize; // bytes, total packet size in bytes
  protected IntWritable flow; // the number of records in raw NetFlow data
  private FlagWritable flag;

  // todo: ICMP

  // For normal (benign) TCP connections:
  private static final long TCP_DURATION_MIN = 1000; // 1000 milliseconds
  private static final int TCP_PACKET_NUMBER_MIN = 3;
  private static final long TCP_AVERAGE_PACKET_SIZE_MIN = 80; // 80 bytes
  protected static final int TCP_FLOW_MAX = 1; // todo: make it higher. 500 is good for 10M flows?

  // For normal (benign) UDP connections:
  private static final int UDP_PACKET_NUMBER_MIN = 3;
  private static final long UDP_AVERAGE_PACKET_SIZE_MIN = 100; // 100 bytes
  protected static final long UDP_FLOW_MAX = 1; // todo: make it higher. 500 is good for 10M flows?

  public NFFeature() {
    // time is set to 0 to show that there is no record there.
    timeStart = new LongWritable(0);
    timeEnd = new LongWritable(0);
    duration = new IntWritable();
    protocol = new IntWritable();
    packetNumber = new IntWritable();
    packetSize = new LongWritable();
    flow = new IntWritable(0);
    flag = new FlagWritable();
  }

  /**
   * @param elements
   * 
   *          <pre>
   * a String array contains the following informations in order:
   *    0: timestamp
   *    1: duration in milliseconds
   *    2: protocol
   *    3: packet number
   *    4: packet size
   *    5: flow
   *    6: flag.
   *          </pre>
   */
  public NFFeature(String[] elements) {
    if (elements[0].equals(IPStatisticsList.NONE)) {
      timeStart = new LongWritable(0);
      timeEnd = new LongWritable(0);
      duration = new IntWritable();
      protocol = new IntWritable();
      packetNumber = new IntWritable();
      packetSize = new LongWritable();
      flow = new IntWritable(0);
      flag = new FlagWritable();
      return;
    }
    try {
      long tempTime = Timestamp.valueOf(elements[0]).getTime();
      timeStart = new LongWritable(tempTime);
      duration = new IntWritable(Integer.parseInt(elements[1]));
      timeEnd = new LongWritable(tempTime + duration.get()); // this is set after duration
      protocol = new IntWritable(Integer.parseInt(elements[2]));
      packetNumber = new IntWritable(Integer.parseInt(elements[3]));
      packetSize = new LongWritable(Long.parseLong(elements[4]));
      flow = new IntWritable(Integer.parseInt(elements[5]));
      flag = new FlagWritable(elements[6]);
    } catch (Exception e) {
      System.err.println("elements[0] = " + elements[0]);
      timeStart = new LongWritable(0);
      timeEnd = new LongWritable(0);
      duration = new IntWritable();
      protocol = new IntWritable();
      packetNumber = new IntWritable();
      packetSize = new LongWritable();
      flow = new IntWritable(0);
      flag = new FlagWritable();
    }
  }

  public NFFeature(NFFeature in) {
    timeStart = in.timeStart;
    timeEnd = in.timeEnd;
    duration = in.duration;
    protocol = in.protocol;
    packetNumber = in.packetNumber;
    packetSize = in.packetSize;
    flow = in.flow;
    flag = in.flag;
  }

  public boolean isBenign() {
    // todo: ICMP
    if (protocol.get() == NFWritable.TCP) {
      // TCP features of flag, flow and duration
      if (flag.hasOnlySYN() || flag.hasOnlyFIN() ||
          flow.get() > TCP_FLOW_MAX ||
          duration.get() < TCP_DURATION_MIN)
        return false;

      // TCP average packet size
      long averagePacketSize = packetSize.get() / packetNumber.get();
      if (packetNumber.get() < TCP_PACKET_NUMBER_MIN &&
          averagePacketSize < TCP_AVERAGE_PACKET_SIZE_MIN)
        return false;

      // End of TCP
    } else if (protocol.get() == NFWritable.UDP) {
      // UDP flow
      if (flow.get() > UDP_FLOW_MAX)
        return false;

      // UDP average packet size
      long averagePacketSize = packetSize.get() / packetNumber.get();
      if (packetNumber.get() < UDP_PACKET_NUMBER_MIN &&
          averagePacketSize < UDP_AVERAGE_PACKET_SIZE_MIN)
        return false;

      // End of UDP
    } // End of testing for different protocols

    // Since no malicious condition is matched, it is benign.
    return true;
  }

  public IntWritable getFlowWritable() {
    return flow;
  }

  public int getFlow() {
    return flow.get();
  }

  public int getProtocol() {
    return protocol.get();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    timeStart.readFields(in);
    timeEnd.readFields(in);
    duration.readFields(in);
    protocol.readFields(in);
    packetNumber.readFields(in);
    packetSize.readFields(in);
    flow.readFields(in);
    flag.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    timeStart.write(out);
    timeEnd.write(out);
    duration.write(out);
    protocol.write(out);
    packetNumber.write(out);
    packetSize.write(out);
    flow.write(out);
    flag.write(out);
  }

  @Override
  public String toString() {
    return new Timestamp(timeStart.get()) + "\t" +
    // timeEnd is ignore, for it can be calculated from timeStart and duration
        duration.toString() + "\t" +
        protocol.toString() + "\t" +
        packetNumber.toString() + "\t" +
        packetSize.toString() + "\t" +
        flow.toString() + "\t" +
        flag.toString();
  }
}

