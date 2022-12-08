package psd.stage5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class ReversedIntWritable
    implements WritableComparable<ReversedIntWritable> {
  private IntWritable value;

  public ReversedIntWritable() {
    value = new IntWritable();
  }

  public ReversedIntWritable(int v) {
    value = new IntWritable(v);
  }

  public int getValue() {
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
  public int compareTo(ReversedIntWritable other) {
    return other.value.compareTo(value);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ReversedIntWritable))
      return false;

    ReversedIntWritable o = (ReversedIntWritable) other;
    return value.equals(o.value);
  }

  @Override
  public int hashCode() {
    return -value.get();
  }

  @Override
  public String toString() {
    return value.toString();
  }
}

