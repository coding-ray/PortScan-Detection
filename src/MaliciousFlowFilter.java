import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaliciousFlowFilter {
  public static final int RECORD_COUNT_LIMIT = 100;

  private static class RecordMapper
      extends Mapper<LongWritable, Text, ConnectionCount, Text> {

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {

      Scanner s = new Scanner(oneLine.toString());
      String ipAndPort = s.next();
      int recordCount = Integer.parseInt(s.next());

      if (recordCount >= RECORD_COUNT_LIMIT)
        context.write(new ConnectionCount(recordCount), new Text(ipAndPort));
    }
  }

  private static class RecordSorter
      extends Reducer<ConnectionCount, Text, Text, ConnectionCount> {

    @Override
    public void reduce(ConnectionCount key, Iterable<Text> values,
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

    job.setMapOutputKeyClass(ConnectionCount.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ConnectionCount.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_3));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_3));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    return job.waitForCompletion(true) ? 0 : 3;
  }
}
