import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

public class ConnectionCount implements WritableComparable<ConnectionCount> {
  private IntWritable value;

  public ConnectionCount() {
    value = new IntWritable();
  }

  public ConnectionCount(int v) {
    value = new IntWritable(v);
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
  public int compareTo(ConnectionCount other) {
    return other.value.compareTo(value);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ConnectionCount))
      return false;

    ConnectionCount o = (ConnectionCount) other;
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
