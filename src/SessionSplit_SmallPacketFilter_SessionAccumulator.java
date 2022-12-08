import java.io.IOException;
import java.util.Iterator;
import java.util.List;
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

public class SessionSplit_SmallPacketFilter_SessionAccumulator {
  private static class SessionSplit_SmallPacketFilter extends
      Mapper<LongWritable, Text, OneWayConnection, SessionAccumulation> {

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {
      IPStatistics[] list = IPStatisticsList.from(oneLine.toString());
      IPPortPair srcKey = list[0].getKey();
      NFFeature srcFeature = list[0].getValue();
      IPPortPair dstKey = list[1].getKey();
      NFFeature dstFeature = list[1].getValue();

      if (srcKey.hasValue() && !srcFeature.isBenign()) {
        context.write(
            new OneWayConnection(srcKey, dstKey),
            new SessionAccumulation(srcFeature));
      }

      if (dstKey.hasValue() && !dstFeature.isBenign()) {
        context.write(
            new OneWayConnection(dstKey, srcKey),
            new SessionAccumulation(dstFeature));
      }
    }
  }

  private static class SessionAccumulator
      extends
      Reducer<OneWayConnection, SessionAccumulation, OneWayConnection, SessionAccumulation> {

    @Override
    public void reduce(OneWayConnection key,
        Iterable<SessionAccumulation> values,
        Context context) throws IOException, InterruptedException {

      SessionAccumulation result = new SessionAccumulation();
      Iterator<SessionAccumulation> iterator = values.iterator();
      while (iterator.hasNext()) {
        SessionAccumulation acc = iterator.next();
        if (result.canAdd(acc)) {
          result.add(acc);
          if (!iterator.hasNext()) // Last session accumulation
            context.write(key, result);
        } else {
          // Cannot add new accumulation to the result
          context.write(key, result);
          result = new SessionAccumulation();
        }
      }
    }
  }

  private static Job initJob()
      throws IOException {
    System.out.println("Stage 2. Skipped.");
    final String jobName = "Stage 3A. Session Split\n" +
        "Stage 3B. Small-Packet Filtering\n" +
        "Stage 3C. Session Accumulation Stage";

    System.out.println(jobName);

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(SessionSplit_SmallPacketFilter_SessionAccumulator.class);

    job.setMapperClass(SessionSplit_SmallPacketFilter.class);
    job.setCombinerClass(SessionAccumulator.class);
    job.setReducerClass(SessionAccumulator.class);

    job.setOutputKeyClass(OneWayConnection.class);
    job.setOutputValueClass(SessionAccumulation.class);

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
