package psd.stage6;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class ReversedLongWritable
    implements WritableComparable<ReversedLongWritable> {
  private LongWritable value;

  public ReversedLongWritable() {
    value = new LongWritable();
  }

  public ReversedLongWritable(int v) {
    value = new LongWritable(v);
  }

  public ReversedLongWritable(String s) {
    value = new LongWritable(Long.parseLong(s));
  }

  public long getValue() {
    return value.get();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    value.write(out);
  }

  @Override
  public int compareTo(ReversedLongWritable other) {
    return other.value.compareTo(value);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ReversedLongWritable))
      return false;

    ReversedLongWritable o = (ReversedLongWritable) other;
    return value.equals(o.value);
  }

  @Override
  public int hashCode() {
    return (int) -(value.get() >> 32);
  }

  @Override
  public String toString() {
    return value.toString();
  }
}

