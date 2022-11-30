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

public class FilterAndSessionExtraction {
	private static class NFMapper
			extends Mapper<LongWritable, Text, NFKey, NFValue> {

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
			extends Reducer<NFKey, NFValue, NFKey, NFSession> {

		/*
		* Pseudo-codes for this reduce function:
		* For each NetFlow record (with the same 2 ends),
		*    if its protocol is neither TCP nor ICMP,
		*        this can not form a session.
		*    else if there is no record in the same direction in this session previously,
		*        this record can be added directly.
		*    else if it is set for the FIN flag of the last record in the same direction in this session,
		*        write this session to the context, and
		*        reset the local session.
		*    else if this connection (record) is of protocol TCP and does not timed out,
		*        combine this record with the original session.
		*    else if there is another record,
		*        write this session to the context, and
		*        reset the local session.
		* End (For loop)
		* write the session to the context.
		*/
		@Override
		public void reduce(NFKey key, Iterable<NFValue> values,
				Context context) throws IOException, InterruptedException {

			NFSessionList list = new NFSessionList(values);

			for (NFSession s : list) {
				if (s.hasValue())
					context.write(key, s);
			}
		}
	}

	private static Job initJob()
			throws IOException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "1. Filter and Session Extraction Stage");
		job.setJarByClass(FilterAndSessionExtraction.class);

		job.setMapperClass(NFMapper.class);
		/* Note that a combiner should not be set!
		 * Otherwise, there will be type mismatch from
		 * the combiner output to the reducer input.
		 */
		job.setReducerClass(SessionCreator.class);

		job.setMapOutputKeyClass(NFKey.class);
		job.setMapOutputValueClass(NFValue.class);
		job.setOutputKeyClass(NFKey.class);
		job.setOutputValueClass(NFSession.class);

		FileInputFormat.addInputPath(job, new Path(IOPath.INPUT_1));
		FileOutputFormat.setOutputPath(job, new Path(IOPath.OUTPUT_1));

		return job;
	}

	public static int run()
			throws IOException, ClassNotFoundException, InterruptedException {

		Job job = initJob();
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
