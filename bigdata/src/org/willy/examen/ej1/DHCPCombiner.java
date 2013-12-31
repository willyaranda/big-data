package org.willy.examen.ej1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
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
public class DHCPCombiner extends
		Reducer<DateWritable, Text, DateWritable, Text> {
	
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
			context.write(key, text);
		}
	}

}
