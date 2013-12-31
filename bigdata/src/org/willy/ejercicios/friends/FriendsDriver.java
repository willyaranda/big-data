package org.willy.ejercicios.friends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.willy.ejercicios.friends.writable.FriendsRelationsWritable;

public class FriendsDriver extends Configured implements Tool {

	/**
	 * This is the main method to start out MR job. Configures everything that
	 * is needed to run the job correctly It also asks for an input and output
	 * path from where it is needed to load and save the files.
	 */
	@Override
	public int run(String[] args) throws Exception {
		String input = args[0] + "/ej2-friends";
		String output = args[1] + "/ej2-friends";

		Path oPath = new Path(output);
		FileSystem.get(oPath.toUri(), getConf()).delete(oPath, true);
		
		Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator"," ");

		// The fun part. Create a job.
        Job job = new Job(conf);

		// Set the driver and the name of the job
		job.setJarByClass(FriendsDriver.class);
		job.setJobName("Ej2-frieds");

		// The input type for the Mapper. See
		// http://hadoop.apache.org/docs/current/api/org/apache/hadoop/mapreduce/lib/input/FileInputFormat.html
		// for more types (CombineFileInputFormat, KeyValueTextInputFormat,
		// NLineInputFormat, SequenceFileInputFormat, TextInputFormat)
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// Set our main classes for the Mapper, Reducer and (optionally)
		// Combiner
		// (Combiner might be the same as the Reducer)
		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FriendsRelationsWritable.class);

		// Set the output type for both key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// We can uncomment this to execute only the mapper
		// (the output will be on the output path)
		// job.setNumReduceTasks(0);

		// Configure the input and output paths
		// (check that input does not have any processing and output
		// is a Path object)
		FileInputFormat.setInputPaths(job, input);
		FileOutputFormat.setOutputPath(job, oPath);

		// Wait for the job to finish and return the status
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.printf("Usage: <input dir> <output dir>");
			return;
		}
		ToolRunner.run(new FriendsDriver(), args);
	}

}
