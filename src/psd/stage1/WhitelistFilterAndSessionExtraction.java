package psd.stage1;

import java.io.IOException;
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
import psd.com.NFValue;
import psd.com.NFWritable;
import psd.com.TwoWayConnection;

public class WhitelistFilterAndSessionExtraction {

  private static class WhitelistFilter
      extends Mapper<LongWritable, Text, TwoWayConnection, NFValue> {

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {
      NFWritable nf = new NFWritable(oneLine.toString());
      if (!nf.isInLocal() && !nf.isInWhitelist())
        context.write(nf.getKey(), nf.getValue());
    }
  }

  private static class SessionCreator
      extends Reducer<TwoWayConnection, NFValue, TwoWayConnection, NFSession> {

    @Override
    public void reduce(TwoWayConnection key, Iterable<NFValue> values,
        Context context) throws IOException, InterruptedException {

      NFSessionList list = new NFSessionList(values);
      for (NFSession session : list) {
        context.write(key, session);
      }
    }
  }

  private static Job initJob()
      throws IOException {
    Configuration conf = new Configuration();
    final String jobName = "Stage 1. Whitelist Filter and Session Extraction";
    System.out.println(jobName);
    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(WhitelistFilterAndSessionExtraction.class);

    job.setMapperClass(WhitelistFilter.class);
    job.setReducerClass(SessionCreator.class);

    job.setMapOutputKeyClass(TwoWayConnection.class);
    job.setMapOutputValueClass(NFValue.class);
    job.setOutputKeyClass(TwoWayConnection.class);
    job.setOutputValueClass(NFValue.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_1));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_1));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    TwoWayConnection.initWhitelist();
    return job.waitForCompletion(true) ? 0 : 1;
  }
}

