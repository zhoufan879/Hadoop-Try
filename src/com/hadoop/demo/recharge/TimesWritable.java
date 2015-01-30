package com.hadoop.demo.recharge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class TimesWritable implements Writable {
	
	private double rmb;
	private int times;
	
	public TimesWritable() {}

	public TimesWritable(double rmb, int times) {
		this.rmb = rmb;
		this.times = times;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(rmb);
		out.writeInt(times);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		rmb = in.readDouble();
		times = in.readInt();
	}

	public double getRmb() {
		return rmb;
	}

	public void setRmb(double rmb) {
		this.rmb = rmb;
	}

	public int getTimes() {
		return times;
	}

	public void setTimes(int times) {
		this.times = times;
	}
}
