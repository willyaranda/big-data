package org.willy.examen.ej1;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.willy.examen.ej1.writables.DateWritable;

public class DHCPMapper extends Mapper<LongWritable, Text, DateWritable, Text> {

	String pattern = "(\\w*)\\s(\\d*)\\s(\\d*):(\\d*):(\\d*)\\s(ktalina dhclient: DHCPREQUEST of)\\s(\\d*\\.\\d*\\.\\d*\\.\\d*).*";

	/**
	 * Arguments depend on the InputFormatClass defined on the Driver. (also
	 * check the specialization of this class!)
	 */
	@Override
	protected void map(LongWritable offset, Text line, Context context)
			throws IOException, InterruptedException {
		/*
		 * We want lines like this: Nov 26 08:05:47 ktalina dhclient:
		 * DHCPREQUEST of 192.168.1.129 on wlan0 to 255.255.255.255 port 67
		 * (xid=0x4df17bbc) Regex 'em!
		 */
		String str = line.toString();

		if (!Pattern.matches(pattern, str)) {
			return;
		}

		String[] strSplitted = str.split("\\s");
		String[] dateSplitted = strSplitted[2].split(":");

		DateWritable date = new DateWritable();
		date.setMonth(new IntWritable(getMonthFromString(strSplitted[0])));
		date.setDay(new IntWritable(Integer.parseInt(strSplitted[1])));
		date.setHour(new IntWritable(Integer.parseInt(dateSplitted[0])));
		date.setMinute(new IntWritable(Integer.parseInt(dateSplitted[1])));
		date.setSecond(new IntWritable(Integer.parseInt(dateSplitted[2])));

		context.write(date, new Text(strSplitted[7]));
	};

	private int getMonthFromString(String month) {
		return 11;
	}

}
