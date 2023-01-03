package psd.stage5;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TypeAndSessionCount
    implements WritableComparable<TypeAndSessionCount> {
  private Text type; // B|V
  private LongWritable sessionCount;

  public TypeAndSessionCount() {
    type = new Text();
    sessionCount = new LongWritable(0);
  }

  public TypeAndSessionCount(String inputType, String inputSessionCount) {
    type = new Text(inputType);
    sessionCount = new LongWritable(Long.parseLong(inputSessionCount));
  }

  public Text getType() {
    return type;
  }

  public long getSessionCount() {
    return sessionCount.get();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    type.readFields(in);
    sessionCount.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    type.write(out);
    sessionCount.write(out);
  }

  @Override
  public int compareTo(TypeAndSessionCount other) {
    if (type.compareTo(other.type) != 0)
      return type.compareTo(other.type);

    return sessionCount.compareTo(other.sessionCount);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof TypeAndSessionCount))
      return false;

    TypeAndSessionCount o = (TypeAndSessionCount) other;
    return type.equals(o.type) &&
        sessionCount.equals(o.sessionCount);
  }

  @Override
  public int hashCode() {
    return type.hashCode() * sessionCount.hashCode();
  }

  @Override
  public String toString() {
    return type.toString() + "\t" + sessionCount.get();
  }

  public Text toText() {
    return new Text(this.toString());
  }
}
