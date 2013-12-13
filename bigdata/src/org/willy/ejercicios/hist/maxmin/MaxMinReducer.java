package org.willy.ejercicios.hist.maxmin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Check the specialization of the Reducer class.
 * The first two classes <b>must</b> be the same as the output
 * from previous maps calls.
 * The two last classes specifies the reducer output (mainly the MR job output),
 * which will be written to disk.
 * @author gll
 *
 */
public class MaxMinReducer extends Reducer<NullWritable, FloatWritable, NullWritable, FloatWritable> {
	/**
	 * See the same for Mapper#setup 
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.setup(context);
	}
	
	/**
	 * Each call to this method receives an unique Key and their values (wrapped in an
	 * Iterable interface).
	 * Should use <code>context.write(key, value)</code> to finish the execution with
	 * this key.
	 */
	@Override
	protected void reduce(NullWritable nil, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		
		Iterator<FloatWritable> it = values.iterator();
		if (!it.hasNext()) {
			return;
		}
		float first = it.next().get();
		
		float min = first;
		float max = first;
		
		for (Iterator<FloatWritable> i = it; i.hasNext();) {
			float elem = i.next().get();
			if (elem < min) {
				min = elem;
			}
			if (elem > max) {
				max = elem;
			}
		}
		
		context.write(NullWritable.get(), new FloatWritable(min));
		context.write(NullWritable.get(), new FloatWritable(max));
	}
	
	/**
	 * See the same for Mapper#cleanup 
	 */
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}
}
