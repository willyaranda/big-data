package org.willy.ejercicios.hist;

import org.apache.hadoop.util.ToolRunner;
import org.willy.ejercicios.hist.maxmin.MaxMinDriver;

public class MainRunner {
	/**
	 * Le program.
	 * First we run the MaxMin MR program.
	 * Then the Histogram one.
	 * And fun.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.printf("Usage: <input dir> <output dir> <# of bars>");
			return;
		}
		ToolRunner.run(new MaxMinDriver(), args);
		ToolRunner.run(new HistDriver(), args);
	}

}
