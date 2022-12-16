package psd.stage1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

import psd.com.NFValue;
import psd.com.NFWritable;

public class NFSession implements Writable {
  private NFValue directConnection;
  private NFValue reverseConnection;

  public static final long TIMEOUT = 60;

  public NFSession() {
    directConnection = new NFValue();
    reverseConnection = new NFValue();
  }

  public NFSession(NFValue input) {
    if (input.getIsReversed()) {
      reverseConnection = new NFValue(input);
      directConnection = new NFValue();
    } else {
      directConnection = new NFValue(input);
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
        reverseConnection = new NFValue(input);
    } else {
      // Deal with directConnection
      if (directConnection.hasValue())
        directConnection.combine(input);
      else
        directConnection = new NFValue(input);
    }
  }

  public boolean canAdd(NFValue input) {
    if ((input.getProtocol() != NFWritable.TCP) &&
        (input.getProtocol() != NFWritable.ICMP))
      return false; // E.g., UDP, for it has no concept of session

    NFValue currentDirection;
    NFValue otherDirection;
    if (input.getIsReversed()) {
      currentDirection = reverseConnection;
      otherDirection = directConnection;
    } else {
      currentDirection = directConnection;
      otherDirection = reverseConnection;
    }

    if (!currentDirection.hasValue())
      return true;

    if (currentDirection.getProtocol() != input.getProtocol())
      return false;

    if (currentDirection.getFlag().hasFIN() ||
        currentDirection.getFlag().hasRST() ||
        otherDirection.getFlag().hasRST())
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

