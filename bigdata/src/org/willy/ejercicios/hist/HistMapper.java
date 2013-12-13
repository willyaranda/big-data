package org.willy.ejercicios.hist;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HistMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
	
	Integer barras = new Integer(1);
	float min = new Integer(0);
	float max = new Integer(0);
	
	/**
	 * This metod *can* be used to share some internal state between map calls.
	 * For example, we can have a "global" fileName variable that can be initialized
	 * just here, and not everytime the map is executed (one per line, depending on the
	 * InputFormatClass defined on the Driver).
	 */
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		barras = context.getConfiguration().getInt("barras", 1);
		min = context.getConfiguration().getFloat("min", 0);
		max = context.getConfiguration().getFloat("max", 0);
	}
	
	/**
	 * Arguments depend on the InputFormatClass defined on the Driver.
	 * (also check the specialization of this class!)
	 */
	@Override
	protected void map(LongWritable offset, Text line, Context context) throws IOException ,InterruptedException {
		//Line is the value
		float value = Float.parseFloat(line.toString());
		int barra = (int) Math.floor( (value-min) / ((max-min)/barras));
		context.write(new IntWritable(barra), NullWritable.get());
	};
	
	/**
	 * Executed once, when all maps calls finish
	 */
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		super.cleanup(context);
	}
	

}
