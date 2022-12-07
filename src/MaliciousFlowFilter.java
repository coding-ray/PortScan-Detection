import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaliciousFlowFilter {
  private static long normalFlowCount = 1;
  // be 500 for 10 million of NetFlow records

  private static final Pattern TAB_PATTERN = Pattern.compile("\t");

  // public static void initNormalSessionRecordCount() throws IOException {
  //   FileSystem fs = FileSystem.get(new Configuration());
  //   Path path = new Path(IOPath.RECORDS_1);
  //   List<String> lines = new ArrayList<>();
  //   try (BufferedReader br = new BufferedReader(
  //       new InputStreamReader(fs.open(path)))) {
  //     String line;
  //     while ((line = br.readLine()) != null) {
  //       lines.add(line);
  //     }
  //   }

  //   normalFlowCount = Long.parseLong(lines.get(0)) / 20000;
  // }

  private static class PortScanVerticalLoader
      extends Mapper<LongWritable, Text, FlowCount, Text> {

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {
      /* oneLine is as follows:
       * 192.168.10.50:0	->	172.16.0.1	8
       */
      String[] elements = TAB_PATTERN.split(oneLine.toString());
      if (elements.length == 0)
        return;

      IPPortPair src = new IPPortPair(elements[0]);
      IPPortPair dst = new IPPortPair(elements[2]);
      int flow = Integer.parseInt(elements[3]);
      context.write(
          new FlowCount(flow),
          new PortScanVertical(src, dst).toText());
    }
  }

  private static class PortScanVerticalFilter
      extends Reducer<FlowCount, Text, Text, FlowCount> {

    @Override
    public void reduce(FlowCount flowCount, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {
      if (flowCount.getValue() <= normalFlowCount)
        return;

      for (Text t : values) {
        context.write(t, flowCount); // Log abnormal IP:port pair
      }
    }
  }

  private static Job initJob()
      throws IOException {
    Configuration conf = new Configuration();
    final String jobName = "5. Malicious Flow Filter";
    System.out.println(jobName);
    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(MaliciousFlowFilter.class);

    job.setMapperClass(PortScanVerticalLoader.class);
    job.setReducerClass(PortScanVerticalFilter.class);

    job.setMapOutputKeyClass(FlowCount.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowCount.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_5));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_5));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    // initNormalSessionRecordCount();
    return job.waitForCompletion(true) ? 0 : 4;
  }
}

