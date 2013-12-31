package org.willy.examen.ej1;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.willy.examen.ej1.writables.DateWritable;

public class DHCPDriver extends Configured implements Tool {

	/**
	 * Main method. We just call ToolRunner to run our driver (see run method
	 * above)
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new DHCPDriver(), args);
	}

	/**
	 * This is the main method to start out MR job. Configures everything that
	 * is needed to run the job correctly It also asks for an input and output
	 * path from where it is needed to load and save the files.
	 */
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: <input dir> <output dir>");
			return -1;
		}

		String input = args[0];
		String output = args[1];

		// Let's create a Path for output and delete if it exists.
		// If we use getLocal, it gets the local FileSystem. If not, it
		// tries to load the hdfs from the conf files from Hadoop.
		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);

		// The fun part. Create a job.
		Job job = new Job();

		// Set the driver and the name of the job
		job.setJarByClass(DHCPDriver.class);
		job.setJobName("DHCP leases");

		// The input type for the Mapper. See
		// http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html
		// for more types (CombineFileInputFormat, KeyValueTextInputFormat,
		// NLineInputFormat, SequenceFileInputFormat, TextInputFormat)
		job.setInputFormatClass(TextInputFormat.class);

		// Set our main classes for the Mapper, Reducer and (optionally)
		// Combiner
		// (Combiner might be the same as the Reducer)
		job.setMapperClass(DHCPMapper.class);
		job.setReducerClass(DHCPReducer.class);
		job.setCombinerClass(DHCPCombiner.class);
		job.setPartitionerClass(DHCPPartitioner.class);

		// Set the output type for both key and value
		job.setOutputKeyClass(DateWritable.class);
		job.setOutputValueClass(Text.class);

		// We can uncomment this to execute only the mapper
		// (the output will be on the output path)
		//job.setNumReduceTasks(0);

		// Configure the input and output paths
		// (check that input does not have any processing and output
		// is a Path object)
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, oPath);
		
		// Defines additional single text based output 'text' for the job
		MultipleOutputs.addNamedOutput(job, "text", TextOutputFormat.class, DateWritable.class, Text.class);

		// Wait for the job to finish and return the status
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

}
