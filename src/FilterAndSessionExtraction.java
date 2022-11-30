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

public class FilterAndSessionExtraction {
	private static class NFMapper
			extends Mapper<LongWritable, Text, NFKey, IntWritable> {

		private static final IntWritable ONE = new IntWritable(1);
		@Override
		public void setup(Context context)
				throws IllegalArgumentException, IOException {
			NFKey.initWhitelist();
		}

		@Override
		public void map(LongWritable key, Text oneLine, Context context)
				throws IOException, InterruptedException {

			NFWritable nf = new NFWritable(oneLine.toString());
			if (!nf.isInWhitelist())
				context.write(nf.getKey(), nf.getValue());
		}
	}

	private static class SessionCreator
			extends Reducer<NFKey, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		@Override
		public void reduce(NFKey key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(new Text(key.toString()), result);
		}
	}

	public static int run()
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "1. Filter and Session Extraction Stage");
		job.setJarByClass(FilterAndSessionExtraction.class);

		job.setMapperClass(NFMapper.class);
		// Note that a combiner should not be set!
		// Otherwise, there will be type mismatch from
		// the combiner output to the reducer input.
		job.setReducerClass(SessionCreator.class);

		job.setMapOutputKeyClass(NFKey.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(
				job, new Path(IOPath.INPUT_1));

		FileOutputFormat.setOutputPath(
				job, new Path(IOPath.OUTPUT_1));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
