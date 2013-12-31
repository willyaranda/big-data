package org.willy.examen.ej1.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class DateWritable extends Configured implements
		WritableComparable<DateWritable> {
		
	private IntWritable month;
	private IntWritable day;
	private IntWritable hour;
	private IntWritable minute;
	private IntWritable second;
	
	public IntWritable getMonth() {
		return month;
	}

	public void setMonth(IntWritable month) {
		this.month = month;
	}

	public IntWritable getDay() {
		return day;
	}

	public void setDay(IntWritable day) {
		this.day = day;
	}

	public IntWritable getHour() {
		return hour;
	}

	public void setHour(IntWritable hour) {
		this.hour = hour;
	}

	public IntWritable getMinute() {
		return minute;
	}

	public void setMinute(IntWritable minute) {
		this.minute = minute;
	}

	public IntWritable getSecond() {
		return second;
	}

	public void setSecond(IntWritable second) {
		this.second = second;
	}

	public DateWritable() {
	}

	public DateWritable(IntWritable month, IntWritable day, IntWritable hour, IntWritable minute,
			IntWritable second) {
		this.month = new IntWritable(month.get());
		this.day = new IntWritable(day.get());
		this.hour = new IntWritable(hour.get());
		this.minute = new IntWritable(minute.get());
		this.second = new IntWritable(second.get());

	}

	@Override
	public int compareTo(DateWritable o) {
		return this.toString().compareTo(o.toString());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		return this.toString().equals(o.toString());
	}

	@Override
	public int hashCode() {
		return month.hashCode() + day.hashCode();
	}
	
	public static DateWritable read(DataInput in) throws IOException {
		DateWritable d = new DateWritable();
		d.readFields(in);
		return d;
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		month = new IntWritable(dataInput.readInt());
		day = new IntWritable(dataInput.readInt());
		hour = new IntWritable(dataInput.readInt());
		minute = new IntWritable(dataInput.readInt());
		second = new IntWritable(dataInput.readInt());
	}

	@Override
	public String toString() {
		return "[" + String.format("%02d", day.get()) + "/" +
	            String.format("%02d", month.get()) + "/" +
				"2013 " +
				String.format("%02d", hour.get()) + ":" +
				String.format("%02d", minute.get()) + ":" +
				String.format("%02d", second.get()) + "]";
		}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.month.write(dataOutput);
		this.day.write(dataOutput);
		this.hour.write(dataOutput);
		this.minute.write(dataOutput);
		this.second.write(dataOutput);
	}
}