package psd.stage3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import psd.com.NFWritable;

/**
 * <pre>
 * Accumulated attributes in NFFeature:
 *    timeStart
 *    timeEnd
 *    duration
 *    packetNumber
 *    packetSize
 *    flow
 * 
 * Cared attribute in NFFeature:
 *    protocol
 * 
 * Ignored attributes in NFFeature:
 *    flag
 *    icmp
 * </pre>
 */
public class SessionAccumulation extends NFFeature {
  // For normal connections
  // todo: make them higher and tunable
  private static final int TCP_SESSION_MAX = 2;
  private static final int UDP_FLOW_MAX = 2;
  private static final int OTHER_FLOW_MAX = 2;

  private IntWritable sessionCount;

  public SessionAccumulation() {
    super();
    sessionCount = new IntWritable(0);
  }

  public SessionAccumulation(NFFeature in) {
    super(in);
    sessionCount = new IntWritable(1);
  }

  /**
   * @param elements A line of output from the session accumulation stage.
   * 
   *          <pre>
   *  0: the time that the source started to connect to the destination
   *  1: the duration in milliseconds that the source has connection to the destination
   *  2: protocol number
   *  3: packet number in total
   *  4: packet size in total
   *  5: number of flow
   *  6: number of session (or the number of flow
   *     for the connection whose protocol is not TCP)
   *          </pre>
   */
  public SessionAccumulation(String[] elements) {
    super(elements);
    if (elements.length == 7)
      sessionCount = new IntWritable(Integer.parseInt(elements[6]));
    else
      sessionCount = new IntWritable(0);
  }

  public boolean canAdd(NFFeature in) {
    return this.isEmpty() || (protocol.get() == in.getProtocol());
  }

  public boolean isEmpty() {
    return timeStart.get() == 0;
  }

  public SessionAccumulation add(SessionAccumulation in) {
    if (this.isEmpty()) {
      // NFFeature
      timeStart = in.timeStart;
      timeEnd = in.timeEnd;
      duration = in.duration;
      packetNumber = in.packetNumber;
      packetSize = in.packetSize;
      flow = in.flow;
      protocol = in.protocol;

      // SessionAccumulation
      sessionCount = in.sessionCount;
      return this;
    }

    // If there is already an accumulation in "this"
    long currentTimeEnd = timeEnd.get();
    int inputDuration = in.duration.get();
    long inputTimeEnd = in.timeEnd.get() + inputDuration;
    if (inputTimeEnd > currentTimeEnd) {
      timeEnd.set(inputTimeEnd);
      duration.set((int) (timeEnd.get() - timeStart.get()));
    }
    packetNumber.set(packetNumber.get() + in.packetNumber.get());
    packetSize.set(packetSize.get() + in.packetSize.get());
    flow.set(flow.get() + in.flow.get());
    sessionCount.set(sessionCount.get() + in.sessionCount.get());
    return this;
  }

  // Assume that the addition is checked by canAdd(a,b)
  public static SessionAccumulation add(
      SessionAccumulation a, SessionAccumulation b) {
    SessionAccumulation result = new SessionAccumulation();
    if (a.isEmpty())
      return b;

    if (b.isEmpty())
      return a;

    result.protocol = a.protocol;

    long aTimeEnd = a.timeEnd.get();
    long bTimeEnd = b.timeEnd.get();
    if (bTimeEnd > aTimeEnd) {
      // b is later than a
      result.timeStart = a.timeStart;
      result.timeEnd = b.timeEnd;
    } else {
      // a is later than or at the same time as b
      result.timeStart = b.timeStart;
      result.timeEnd = a.timeEnd;
    }

    result.duration = new IntWritable(
        (int) (result.timeEnd.get() - result.timeStart.get()));

    result.packetNumber = new IntWritable(
        a.packetNumber.get() + b.packetNumber.get());

    result.packetSize = new LongWritable(
        a.packetSize.get() + b.packetSize.get());

    result.flow = new IntWritable(a.flow.get() + b.flow.get());

    result.sessionCount = new IntWritable(
        a.sessionCount.get() + b.sessionCount.get());

    return result;
  }

  public boolean hasNormalFlow() {
    if (protocol.get() == NFWritable.TCP)
      return sessionCount.get() <= TCP_SESSION_MAX;
    else if (protocol.get() == NFWritable.UDP)
      return flow.get() <= UDP_FLOW_MAX;
    else
      return flow.get() <= OTHER_FLOW_MAX;
  }

  public IntWritable getSessionCountWritable() {
    return sessionCount;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // NFFeature
    timeStart.readFields(in);
    timeEnd.readFields(in);
    duration.readFields(in);
    protocol.readFields(in);
    packetNumber.readFields(in);
    packetSize.readFields(in);
    flow.readFields(in);

    // SessionAccumulation
    sessionCount.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // NFFeature
    timeStart.write(out);
    timeEnd.write(out);
    duration.write(out);
    protocol.write(out);
    packetNumber.write(out);
    packetSize.write(out);
    flow.write(out);

    // SessionAccumulation
    sessionCount.write(out);
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
        sessionCount.toString();
  }
}
