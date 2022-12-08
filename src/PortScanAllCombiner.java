import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PortScanAllCombiner {

  private static class SessionAccumulationLoader extends
      Mapper<LongWritable, Text, ReversedIntWritable, PortScanVerticalConnection> {

    private static final Pattern TAB_PATTERN = Pattern.compile("\t");

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {
      /* Sample input (oneLine) and the index of elemets after split
       * 0                  1   2               3                       5 6 7   8     9 10
       * 192.168.10.9:1033	->	192.168.10.3:88	2021-04-11 09:40:58.072	2	6	20	6992	2	2
       * 
       * 0: source IP and port
       * 1: ->, which indicates the flow is from 0 to 2
       * 2: destination IP and port
       * 3: the time that the source started to connect to the destination
       * 4: the duration in milliseconds that the source has connection to the destination
       * 5: protocol number
       * 6: packet number in total
       * 7: packet size in total
       * 8: number of flow
       * 9: number of session (or the number of flow for the connection whose protocol is not TCP)
       */

      String[] elements = TAB_PATTERN.split(oneLine.toString());
      if (!elements[3].equals(IPStatisticsList.NONE))
        context.write(
            new ReversedIntWritable(Integer.parseInt(elements[9])),
            new PortScanVerticalConnection(elements[0], elements[2]));
    }
  } // End of mapper

  private static class ConnectionSorter extends
      Reducer<ReversedIntWritable, PortScanVerticalConnection, PortScanVerticalConnection, ReversedIntWritable> {

    @Override
    public void reduce(ReversedIntWritable key,
        Iterable<PortScanVerticalConnection> values, Context context)
        throws IOException, InterruptedException {

      for (PortScanVerticalConnection connection : values) {
        context.write(connection, key);
      }
    }
  } // End of reducer

  private static Job initJob()
      throws IOException {
    final String jobName = "Stage 5. Sorting by Session Count";
    System.out.println(jobName);

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(PortScanAllCombiner.class);

    job.setMapperClass(SessionAccumulationLoader.class);
    job.setReducerClass(ConnectionSorter.class);

    job.setMapOutputKeyClass(ReversedIntWritable.class);
    job.setMapOutputValueClass(PortScanVerticalConnection.class);
    job.setOutputKeyClass(PortScanVerticalConnection.class);
    job.setOutputValueClass(ReversedIntWritable.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_5));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_5));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    return job.waitForCompletion(true) ? 0 : 5;
  }
}
