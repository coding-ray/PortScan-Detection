import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class NFSession implements Writable {
  private NFValue directConnection;
  private NFValue reverseConnection;

  // TCP timeout is set to 1 hour in milliseconds
  // todo: it should be lower.
  public static final long TIMEOUT = 3600L * 1000;

  public NFSession() {
    directConnection = new NFValue();
    reverseConnection = new NFValue();
  }

  public NFSession(NFValue input) {
    if (input.getIsReversed()) {
      reverseConnection = input;
      directConnection = new NFValue();
    } else {
      directConnection = input;
      reverseConnection = new NFValue();
    }
  }

  /**
   * Must check if it can be added with canAdd() before adding!
   */
  public void add(NFValue input) {
    if (input.getIsReversed()) {
      // Deal with reverseConnection
      if (reverseConnection.hasValue())
        reverseConnection.combine(input);
      else
        reverseConnection = input;
    } else {
      // Deal with directConnection
      if (directConnection.hasValue())
        directConnection.combine(input);
      else
        directConnection = input;
    }
  }

  public boolean canAdd(NFValue input) {
    // todo: test all the conditions in this function
    if ((input.getProtocol() != NFWritable.TCP) &&
        (input.getProtocol() != NFWritable.ICMP))
      return false;

    NFValue currentDirection = input.getIsReversed() ? reverseConnection
        : directConnection;

    if (!currentDirection.hasValue())
      return true;

    if (currentDirection.getFlag().hasFIN())
      return false;

    if (input.getProtocol() == NFWritable.TCP) {
      long inputTime = input.getTime();
      long currentTime = currentDirection.getTime();
      long currentDuration = currentDirection.getDuration();
      return (inputTime - (currentTime + currentDuration)) < TIMEOUT;
    } else
      return false;
  }

  public boolean hasValue() {
    return (directConnection.hasValue()) || (reverseConnection.hasValue());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    directConnection.readFields(in);
    reverseConnection.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    directConnection.write(out);
    reverseConnection.write(out);
  }

  @Override
  public String toString() {
    return directConnection.toString() + "\t" + reverseConnection.toString();
  }
}
