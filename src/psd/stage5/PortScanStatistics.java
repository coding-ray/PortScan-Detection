package psd.stage5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import psd.com.IPPortPair;
import psd.com.TwoWayConnection;
import psd.stage4.block.PortScanBlockConnection;
import psd.stage4.vertical.PortScanVerticalConnection;

public class PortScanStatistics implements Writable {
  private LongWritable count;

  public PortScanStatistics() {
    count = new LongWritable(0);
  }

  public void add(TypeAndSessionCount in) {
    count = new LongWritable(count.get() + in.getSessionCount());
  }

  public long getCount() {
    return count.get();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    count.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    count.write(out);
  }

  @Override
  public String toString() {
    return Long.toString(count.get());
  }

  public Text toText() {
    return new Text(this.toString());
  }
}

