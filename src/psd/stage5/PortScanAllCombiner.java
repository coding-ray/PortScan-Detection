package psd.stage5;

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

public class PortScanAllCombiner {

  private static class SessionAccumulationLoader extends
      Mapper<LongWritable, Text, PortScanClassBConnection, TypeAndSessionCount> {

    private static final Pattern TAB_PATTERN = Pattern.compile("\t");

    @Override
    public void map(LongWritable key, Text oneLine, Context context)
        throws IOException, InterruptedException {
      /* 2 sample input (oneLine) and the index of elemets after split
       * 0	1									2		3								4												5	6	7		8			9	10
       * V	192.168.10.9:1033	->	192.168.10.3	2021-04-11 09:40:58.072	2	6	20	6992	2	2
       * B	192.168.10.9:1033	->	192.168/16 	  2021-04-11 09:40:58.072	2	6	20	6992	2	2
       * 
       * 0 : V|H|B, which denodes this record is a vertical, horizontal or block scan accumulation
       * 1 : source IP and port
       * 2 : ->, which indicates the flow is from 0 to 2
       * 3 : destination IP and port
       * 4 : the time that the source started to connect to the destination
       * 5 : the duration in milliseconds that the source has connection to the destination
       * 6 : protocol number
       * 7 : packet number in total
       * 8 : packet size in total
       * 9 : number of flow
       * 10: number of session (or the number of flow for the connection whose protocol is not TCP)
       */

      // todo: combine 3 kinds of port scans
      String[] elements = TAB_PATTERN.split(oneLine.toString());
      context.write(
          new PortScanClassBConnection(elements[1], elements[3]),
          new TypeAndSessionCount(elements[0], elements[10]));
    }
  } // End of mapper

  private static class PortScanCombiner extends
      Reducer<PortScanClassBConnection, TypeAndSessionCount, PortScanClassBConnection, PortScanStatistics> {

    @Override
    public void reduce(PortScanClassBConnection key,
        Iterable<TypeAndSessionCount> values, Context context)
        throws IOException, InterruptedException {

      PortScanStatistics statistics = new PortScanStatistics();
      for (TypeAndSessionCount typeAndSessionCount : values) {
        statistics.add(typeAndSessionCount);
      }
      if (statistics.getCount() >= 10)
        context.write(key, statistics);
    }
  } // End of reducer

  private static Job initJob()
      throws IOException {
    final String jobName = "Stage 5. Combine All Port-Scan Types";
    System.out.println(jobName);

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, jobName);
    job.setJarByClass(PortScanAllCombiner.class);

    job.setMapperClass(SessionAccumulationLoader.class);
    job.setReducerClass(PortScanCombiner.class);

    job.setMapOutputKeyClass(PortScanClassBConnection.class);
    job.setMapOutputValueClass(TypeAndSessionCount.class);
    job.setOutputKeyClass(PortScanClassBConnection.class);
    job.setOutputValueClass(PortScanStatistics.class);

    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_5_BLOCK));
    FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_5_VERTICAL));
    FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_5));

    return job;
  }

  public static int run()
      throws IOException, ClassNotFoundException, InterruptedException {

    Job job = initJob();
    return job.waitForCompletion(true) ? 0 : 5;
  }
}
