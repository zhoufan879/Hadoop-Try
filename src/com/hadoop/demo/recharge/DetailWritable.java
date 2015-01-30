package com.hadoop.demo.recharge;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class DetailWritable implements WritableComparable<DetailWritable> {
	
	private String name;
	private String phone;
	private int date;
	
	public DetailWritable() {}
	
	public DetailWritable(String name, String phone, int date) {
		this.name = name;
		this.phone = phone;
		this.date = date;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, name);
		WritableUtils.writeString(out, phone);
		WritableUtils.writeVInt(out, date);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = WritableUtils.readString(in);
		phone = WritableUtils.readString(in);
		date = WritableUtils.readVInt(in);
	}

	@Override
	public int compareTo(DetailWritable o) {
		int c = this.name.compareTo(o.name);
		if(c != 0){
			return c;
		}
		return this.date < o.getDate() ? -1 : ( this.date == o.getDate() ? 0 : 1);
//		return this.name.compareTo(o.name);
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof DetailWritable){
			DetailWritable o = (DetailWritable) obj;
			return this.name.equals(o.getName()) 
					&& this.phone.equals(o.getPhone()) 
					&& this.date == o.date;
		}
		return false;
	}
	
	@Override
	public int hashCode() {
		return this.name.hashCode() 
				+ this.phone.hashCode() 
				+ this.date;
	}

	@Override
	public String toString() {
		return name + "\t" + phone + "\t" + date;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	public int getDate() {
		return date;
	}

	public void setDate(int date) {
		this.date = date;
	}

}
