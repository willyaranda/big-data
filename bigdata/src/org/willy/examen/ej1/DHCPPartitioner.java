package org.willy.examen.ej1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.willy.examen.ej1.writables.DateWritable;

public class DHCPPartitioner extends Partitioner<DateWritable, Text> {

	@Override
	public int getPartition(DateWritable date, Text value, int numPartitions) {
		int partition = ((date.getMonth().hashCode() + date.getDay().hashCode()) ^ 31)
				% numPartitions;
		System.out.println("In partitioner, returning " + partition
				+ " out of " + numPartitions);
		return partition;
	}

}
