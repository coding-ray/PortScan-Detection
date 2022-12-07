import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class NFFeature implements Writable {
  // Basic features
  private LongWritable timeStart; // the time that the host starts to send data in milliseconds
  private LongWritable timeEnd; // the time that the host finish sending data in milliseconds
  private IntWritable duration; // in milliseconds
  private IntWritable protocol; // 1: ICMP, 2: IGMP, 6: TCP, 17: UDP
  private IntWritable packetNumber; // packets, total packet number
  private LongWritable packetSize; // bytes, total packet size in bytes
  private IntWritable flow; // the number of records in raw NetFlow data
  private FlagWritable flag;

  // ICMP is ignore here

  // For normal (benign) connections:
  private static final long TCP_DURATION_MIN = 1000; // 1000 milliseconds
  private static final int TCP_PACKET_NUMBER_MIN = 3;
  private static final long TCP_AVERAGE_PACKET_SIZE_MIN = 80; // 80 bytes
  private static final int UDP_PACKET_NUMBER_MIN = 3;
  private static final long UDP_AVERAGE_PACKET_SIZE_MIN = 100; // 100 bytes

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
    long tempTime = Timestamp.valueOf(elements[0]).getTime();
    timeStart = new LongWritable(tempTime);
    duration = new IntWritable(Integer.parseInt(elements[1]));
    timeEnd = new LongWritable(tempTime + duration.get()); // this is set after duration
    protocol = new IntWritable(Integer.parseInt(elements[2]));
    packetNumber = new IntWritable(Integer.parseInt(elements[3]));
    packetSize = new LongWritable(Long.parseLong(elements[4]));
    flow = new IntWritable(Integer.parseInt(elements[5]));
    flag = new FlagWritable(elements[6]);
  }

  public NFFeature add(NFFeature in) {
    System.out.println("NFFeature.<init>: flow original, input = " + flow.get() + ", " + in.flow.get());
    if (flow.get() == 0) {
      timeStart = in.timeStart;
      timeEnd = in.timeEnd;
      duration = in.duration;
      protocol = in.protocol;
      packetNumber = in.packetNumber;
      packetSize = in.packetSize;
      flow.set(in.flow.get());
      flag = in.flag;
      // flagList = in.flagList;
      return this;
    }

    // If there is already a feature in "this"
    long currentTimeEnd = timeEnd.get();
    int inputDuration = in.duration.get();
    long inputTimeEnd = in.timeEnd.get() + inputDuration;
    if (inputTimeEnd > currentTimeEnd) {
      timeEnd.set(inputTimeEnd);
      duration.set((int) (timeEnd.get() - timeStart.get()));
    }
    packetNumber.set(packetNumber.get() + in.packetNumber.get());
    packetSize.set(packetSize.get() + in.packetSize.get());
    System.out.println("NFFeature: flow is to be added by" + in.flow.get());
    flow.set(flow.get() + in.flow.get());
    flag.overlapWith(in.flag);
    return this;

    // int flagListLength = in.flagList.size();
    // FlagWritable[] inputFlags = new FlagWritable[flagListLength];
    // System.out.println(in.flagList.size());
    // in.flagList.get().toArray(inputFlags);
    // for (int index = 0; index < flagListLength; index++) {
    //   // flagList.add(inputFlags[index]);
    // }
  }

  public boolean isBenign() {
    if (protocol.get() == NFWritable.TCP) {
      if (flag.hasOnlySYN() || flag.hasOnlyFIN())
        return false;

      long averagePacketSize = packetSize.get() / packetNumber.get();
      if (duration.get() < TCP_DURATION_MIN &&
          packetNumber.get() < TCP_PACKET_NUMBER_MIN &&
          averagePacketSize < TCP_AVERAGE_PACKET_SIZE_MIN)
        return false;

    } else if (protocol.get() == NFWritable.UDP) {
      long averagePacketSize = packetSize.get() / packetNumber.get();
      if (packetNumber.get() < UDP_PACKET_NUMBER_MIN &&
          averagePacketSize < UDP_AVERAGE_PACKET_SIZE_MIN)
        return false;
    }

    return true;
  }

  public IntWritable getFlowWritable() {
    return flow;
  }

  public int getFlow() {
    return flow.get();
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
        new Timestamp(timeEnd.get()) + "\t" +
        duration.toString() + "\t" +
        protocol.toString() + "\t" +
        packetNumber.toString() + "\t" +
        packetSize.toString() + "\t" +
        flow.toString() + "\t" +
        flag.toString();
  }
}

