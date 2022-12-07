import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class FlagList implements Iterable<FlagWritable>, Writable {
   private IntWritable length;
   private List<FlagWritable> list;

   public FlagList() {
      length = new IntWritable(0);
      list = new ArrayList<>();
   }

   @Override
   public Iterator<FlagWritable> iterator() {
      return list.iterator();
   }

   @Override
   public void readFields(DataInput in) throws IOException {
      length.readFields(in);
      for (int dummy = 0; dummy < length.get(); dummy++) {
         FlagWritable tempFlag = new FlagWritable();
         tempFlag.readFields(in);
         list.add(tempFlag);
      }
   }

   @Override
   public void write(DataOutput out) throws IOException {
      length.write(out);
      for (FlagWritable flag : list) {
         flag.write(out);
      }
   }

   public void add(FlagWritable inputFlag) {
      list.add(inputFlag);
      length.set(length.get() + 1);
   }

   public List<FlagWritable> get() {
      return list;
   }

   public int size() {
      return list.size();
   }

   @Override
   public String toString() {
      StringBuilder output = new StringBuilder();
      for (FlagWritable flag : list) {
         output.append(flag.toString() + " ");
      }
      // Remove the last space
      output.deleteCharAt(output.length() - 1);
      return output.toString();
   }
}

