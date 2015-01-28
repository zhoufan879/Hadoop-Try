package com.hadoop.demo.surfer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class CostWritable implements Writable {
	
	private double ll;
	private double cost;
	
	public CostWritable() {}
	
	public CostWritable(double ll, double cost) {
		this.ll = ll;
		this.cost = cost;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(ll);
		out.writeDouble(cost);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.ll = in.readDouble();
		this.cost = in.readDouble();
	}

	@Override
	public String toString() {
		return ll + "\t" + cost;
	}
	
	
}
