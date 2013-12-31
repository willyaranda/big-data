package org.willy.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Check the specialization of the Reducer class. The first two classes
 * <b>must</b> be the same as the output from previous maps calls. The two last
 * classes specifies the reducer output (mainly the MR job output), which will
 * be written to disk.
 * 
 * @author gll
 * 
 */
public class WCReducer extends Reducer<Text, NullWritable, Text, IntWritable> {
	/**
	 * See the same for Mapper#cleanup
	 */
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}

	/**
	 * Each call to this method receives an unique Key and their values (wrapped
	 * in an Iterable interface). Should use
	 * <code>context.write(key, value)</code> to finish the execution with this
	 * key.
	 */
	@SuppressWarnings("unused")
	@Override
	protected void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		int contador = 0;
		for (NullWritable unused : values) {
			contador++;
		}
		context.write(key, new IntWritable(contador));
	}

	/**
	 * See the same for Mapper#setup
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
}
