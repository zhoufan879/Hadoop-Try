package com.hadoop.demo.expense;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class DetailWritable implements WritableComparable<DetailWritable> {
	
	private String name;
	private String date;
	
	public DetailWritable() {}
	
	public DetailWritable(String name, String date) {
		this.name = name;
		this.date = date;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = WritableUtils.readString(in);
		date = WritableUtils.readString(in);
	}

	@Override
	public int compareTo(DetailWritable o) {
		int c = this.name.compareTo(o.name);
		if(c != 0){
			return c;
		}
		
		c = this.date.compareTo(o.date);
		if(c != 0){
			return c;
		}
		
		return 0;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DetailWritable){
			DetailWritable o = (DetailWritable) obj;
			return this.name.equals(o.name) 
					&& this.date == o.date;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.name.hashCode() + this.date.hashCode();
	}

	@Override
	public String toString() {
		return name + "\t" + date;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}
}
