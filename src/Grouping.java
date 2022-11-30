import java.io.IOException;
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

public class Grouping {
  private static class IPMapper
      extends Mapper<LongWritable, Text, IPPortPair, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {

      IPStatisticsList list = new IPStatisticsList(oneLine.toString());
      for (IPStatistics s : list) {
        context.write(s.getKey(), ONE);
      }
    }
  }

  private static class IPStatisticsCreator
      extends Reducer<IPPortPair, IntWritable, IPPortPair, IntWritable> {

    @Override
    public void reduce(IPPortPair key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      int sum = 0;
      for (IntWritable i : values) {
        sum += i.get();
      }

      context.write(key, new IntWritable(sum));
    }
  }

  private static Job initJob()
      throws IOException {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "2. Grouping Stage");
    job.setJarByClass(Grouping.class);

    job.setMapperClass(IPMapper.class);
    job.setCombinerClass(IPStatisticsCreator.class);
    job.setReducerClass(IPStatisticsCreator.class);

    // job.setMapOutputKeyClass(IPPortPair.class);
    // job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IPPortPair.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_2));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_2));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    return job.waitForCompletion(true) ? 0 : 2;
  }
}
