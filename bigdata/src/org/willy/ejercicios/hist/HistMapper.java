package org.willy.ejercicios.hist;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HistMapper extends
		Mapper<LongWritable, Text, BarWritable, NullWritable> {

	Integer barras = new Integer(1);
	float min = new Integer(0);
	float max = new Integer(0);

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
		// Line is the value. Parse it.
		float value = Float.parseFloat(line.toString());
		float diff = max - min;
		int barra = (int) Math.floor((value - min) / (diff / barras));
		
		// Max value will not be in latest bar. Workaround it.
		if (value == max) {
			barra = barra-1;
		}
		
		if (value == min) {
			barra = 0;
		}
		
		BarWritable b = new BarWritable();
		float left = min + (barra*(diff/barras));
		b.setLeft(new FloatWritable(left));
		float right = min + (barra*(diff/barras)) + (diff/barras);
		b.setRight(new FloatWritable(right));
		context.write(b, NullWritable.get());
	};

	/**
	 * This method *can* be used to share some internal state between map calls.
	 * For example, we can have a "global" fileName variable that can be
	 * initialized just here, and not everytime the map is executed (one per
	 * line, depending on the InputFormatClass defined on the Driver).
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		barras = context.getConfiguration().getInt("barras", 1);
		min = context.getConfiguration().getFloat("min", 0);
		max = context.getConfiguration().getFloat("max", 0);
	}

}
