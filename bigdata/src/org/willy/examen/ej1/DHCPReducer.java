package org.willy.examen.ej1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.willy.examen.ej1.writables.DateWritable;

/**
 * Check the specialization of the Reducer class. The first two classes
 * <b>must</b> be the same as the output from previous maps calls. The two last
 * classes specifies the reducer output (mainly the MR job output), which will
 * be written to disk.
 * 
 * @author gll
 * 
 */
public class DHCPReducer extends
		Reducer<DateWritable, Text, DateWritable, Text> {

	String generateFileName(DateWritable k, Text v) {
		return "part-dia-" + k.getDay();
	}

	private MultipleOutputs<DateWritable, Text> mos;

	@Override
	public void setup(Context context) {
		mos = new MultipleOutputs<DateWritable, Text>(context);
	}

	/**
	 * Each call to this method receives an unique Key and their values (wrapped
	 * in an Iterable interface). Should use
	 * <code>context.write(key, value)</code> to finish the execution with this
	 * key.
	 */
	@Override
	protected void reduce(DateWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for (Text text : values) {
			mos.write(key, text, generateFileName(key, text));
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		mos.close();
	}

}
