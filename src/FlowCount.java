import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

public class FlowCount implements WritableComparable<FlowCount> {
  private IntWritable value;

  public FlowCount() {
    value = new IntWritable();
  }

  public FlowCount(int v) {
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
  public int compareTo(FlowCount other) {
    return other.value.compareTo(value);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FlowCount))
      return false;

    FlowCount o = (FlowCount) other;
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

