import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class IPStatistics implements Writable {
  private IPPortPair key;
  private NFFeature value;

  public IPStatistics() {
    key = new IPPortPair();
    value = new NFFeature();
  }

  public IPStatistics(IPPortPair inputKey, NFFeature inputValue) {
    this.key = inputKey;
    this.value = inputValue;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    key.readFields(in);
    value.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    key.write(out);
    value.write(out);
  }

  public IPPortPair getKey() {
    return key;
  }

  public NFFeature getValue() {
    return value;
  }
}

