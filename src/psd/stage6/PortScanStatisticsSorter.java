package psd.stage6;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import psd.com.IOPath;
import psd.stage5.PortScanClassBConnection;

public class PortScanStatisticsSorter {

  private static class SessionAccumulationLoader extends
      Mapper<LongWritable, Text, ReversedLongWritable, PortScanClassBConnection> {

    private static final Pattern TAB_PATTERN = Pattern.compile("\t");

    /* 2 sample input (oneLine) and the index of elemets after split
       * 0                1   2             3	
       * 172.217.11.3:443	->	192.168/16	  28
       * 172.217.11.3:443	->	192.168.10.5	7
       */
    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {

      String[] elements = TAB_PATTERN.split(oneLine.toString());
      context.write(
          new ReversedLongWritable(elements[3]),
          new PortScanClassBConnection(elements[0], elements[2]));
    }
  } // End of mapper

  private static class PortScanCombiner extends
      Reducer<ReversedLongWritable, PortScanClassBConnection, PortScanClassBConnection, ReversedLongWritable> {

    @Override
    public void reduce(ReversedLongWritable key,
        Iterable<PortScanClassBConnection> values, Context context)
        throws IOException, InterruptedException {

      for (PortScanClassBConnection val : values) {
        context.write(val, key);
      }
    }
  } // End of reducer

  private static Job initJob()
      throws IOException {
    final String jobName = "Stage 6. Sort Statistics";
    System.out.println(jobName);

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(PortScanStatisticsSorter.class);

    job.setMapperClass(SessionAccumulationLoader.class);
    job.setReducerClass(PortScanCombiner.class);

    job.setMapOutputKeyClass(ReversedLongWritable.class);
    job.setMapOutputValueClass(PortScanClassBConnection.class);
    job.setOutputKeyClass(PortScanClassBConnection.class);
    job.setOutputValueClass(ReversedLongWritable.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_6));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_6));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    return job.waitForCompletion(true) ? 0 : 6;
  }
}
