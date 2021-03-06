package org.willy.ejercicios.hist.writables;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;

public class BarWritable extends Configured implements
		WritableComparable<BarWritable> {
	private FloatWritable left;
	private FloatWritable right;

	public BarWritable() {
		this.left = new FloatWritable(Float.MIN_VALUE);
		this.right = new FloatWritable(Float.MAX_VALUE);
	}

	public BarWritable(FloatWritable left, FloatWritable right) {
		this.left = new FloatWritable(left.get());
		this.right = new FloatWritable(right.get());
	}

	@Override
	public int compareTo(BarWritable o) {
		return this.right.compareTo(o.right);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		BarWritable that = (BarWritable) o;

		if (right != null ? !right.equals(that.right) : that.right != null) {
			return false;
		}
		if (left != null ? !left.equals(that.left) : that.left != null) {
			return false;
		}

		return true;
	}

	public FloatWritable getLeft() {
		return left;
	}

	public FloatWritable getRight() {
		return right;
	}

	@Override
	public int hashCode() {
		return new Float(left.get()).hashCode() ^ new Double(right.get()).hashCode();
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.left.readFields(dataInput);
		this.right.readFields(dataInput);
	}

	public void setLeft(FloatWritable left) {
		this.left = left;
	}

	public void setRight(FloatWritable right) {
		this.right = right;
	}

	@Override
	public String toString() {
		return "[" + this.left + "," + this.right + ")";
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		this.left.write(dataOutput);
		this.right.write(dataOutput);
	}
}