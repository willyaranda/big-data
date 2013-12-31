package org.willy.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	/**
	 * Executed once, when all maps calls finish
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	/**
	 * Arguments depend on the InputFormatClass defined on the Driver. (also
	 * check the specialization of this class!)
	 */
	@Override
	protected void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {
		StringTokenizer st = new StringTokenizer(line.toString());
		while (st.hasMoreTokens()) {
			context.write(new Text(st.nextToken()), NullWritable.get());
		}
	};

	/**
	 * This metod *can* be used to share some internal state between map calls.
	 * For example, we can have a "global" fileName variable that can be
	 * initialized just here, and not everytime the map is executed (one per
	 * line, depending on the InputFormatClass defined on the Driver).
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}

}
