import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

public class FlagWritable implements Writable {
  private BooleanWritable urg;
  private BooleanWritable ack;
  private BooleanWritable psh;
  private BooleanWritable rst;
  private BooleanWritable syn;
  private BooleanWritable fin;

  // Default constructor for Hadoop to instanciation
  public FlagWritable() {
    urg = new BooleanWritable();
    ack = new BooleanWritable();
    psh = new BooleanWritable();
    rst = new BooleanWritable();
    syn = new BooleanWritable();
    fin = new BooleanWritable();
  }

  // Custom constructor for flags in the format of
  // 0x?? or UAPRSF.
  public FlagWritable(String flagString) {
    if (flagString.charAt(0) == '0') {
      /*
       * The input is of format CEUAPRSF in hexidecimal.
       * E.g., 0x5b.
       * Note that there are additional 2 ECN bits (CWR, ECE)
       * ignored in this project.
       */
      int f = Integer.parseInt(flagString.substring(2), 16);
      urg = new BooleanWritable((f & 32) != 0);
      ack = new BooleanWritable((f & 16) != 0);
      psh = new BooleanWritable((f & 8) != 0);
      rst = new BooleanWritable((f & 4) != 0);
      syn = new BooleanWritable((f & 2) != 0);
      fin = new BooleanWritable((f & 1) != 0);
    } else {
      // The input is of format UAPRSF.
      char[] flagArray = flagString.toCharArray();
      urg = new BooleanWritable(flagArray[0] != '.');
      ack = new BooleanWritable(flagArray[1] != '.');
      psh = new BooleanWritable(flagArray[2] != '.');
      rst = new BooleanWritable(flagArray[3] != '.');
      syn = new BooleanWritable(flagArray[4] != '.');
      fin = new BooleanWritable(flagArray[5] != '.');
    }
  }

  public FlagWritable(BooleanWritable u, BooleanWritable a, BooleanWritable p,
      BooleanWritable r, BooleanWritable s, BooleanWritable f) {
    urg = u;
    ack = a;
    psh = p;
    rst = r;
    syn = s;
    fin = f;
  }

  public void set(boolean u, boolean a, boolean p, boolean r, boolean s,
      boolean f) {
    urg.set(u);
    ack.set(a);
    psh.set(p);
    rst.set(r);
    syn.set(s);
    fin.set(f);
  }

  @Override
  // Overriding default readFields method.
  // It de-serializes the byte stream data
  public void readFields(DataInput in) throws IOException {
    urg.readFields(in);
    ack.readFields(in);
    psh.readFields(in);
    rst.readFields(in);
    syn.readFields(in);
    fin.readFields(in);
  }

  @Override
  // It serializes object data into byte stream data
  public void write(DataOutput out) throws IOException {
    urg.write(out);
    ack.write(out);
    psh.write(out);
    rst.write(out);
    syn.write(out);
    fin.write(out);
  }
}
