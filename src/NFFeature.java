import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class NFFeature implements Writable {
  public NFFeature() {
    // todo
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
   *    6: flag
   *          </pre>
   */
  public NFFeature(String[] elements) {
    // todo
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    // timestamp.readFields(in);
    // todo
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // timestamp.write(out);
    // todo
  }

  @Override
  public String toString() {
    return new String(); // todo
  }
}
