
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
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
  private static long normalSessionRecordCount;
  // be 500 for 10 million of NetFlow records

  public static void initNormalSessionRecordCount() throws IOException {
    FileSystem fs = FileSystem.get(new Configuration());
    Path path = new Path(IOPath.RECORDS_1);
    List<String> lines = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(
        new InputStreamReader(fs.open(path)))) {
      String line;
      while ((line = br.readLine()) != null) {
        lines.add(line);
      }
    }

    normalSessionRecordCount = Long.parseLong(lines.get(0)) / 20000;
  }

  private static class RecordMapper
      extends Mapper<LongWritable, Text, FlowCount, Text> {

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {

      try (Scanner s = new Scanner(oneLine.toString());) {
        String ipAndPort = s.next();
        int recordCount = Integer.parseInt(s.next());

        if (recordCount >= normalSessionRecordCount)
          context.write(new FlowCount(recordCount), new Text(ipAndPort));
      } // End of try
    }
  }

  private static class RecordSorter
      extends Reducer<FlowCount, Text, Text, FlowCount> {

    @Override
    public void reduce(FlowCount key, Iterable<Text> values,
        Context context) throws IOException, InterruptedException {

      for (Text t : values) {
        context.write(t, key);
      }

    }
  }

  private static Job initJob()
      throws IOException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "3. Malicious Flow Filter");
    job.setJarByClass(MaliciousFlowFilter.class);

    job.setMapperClass(RecordMapper.class);
    job.setReducerClass(RecordSorter.class);

    job.setMapOutputKeyClass(FlowCount.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FlowCount.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_3));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_3));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    initNormalSessionRecordCount();
    return job.waitForCompletion(true) ? 0 : 3;
  }
}
