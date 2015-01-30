package com.hadoop.demo.expense;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.demo.surfer.Arith;

public class MyCombine extends Reducer<DetailWritable, DoubleWritable, DetailWritable, DoubleWritable> {
	@Override
	protected void reduce(DetailWritable key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
		System.out.print("Combine [ " + key + " :");
		
		double t1 = 0.00d;
		for (DoubleWritable dw : value) {
			t1 = Arith.add(t1, dw.get());
			System.out.print(" + " + dw.get());
		}
		System.out.println(" = " + t1 + " ]");
		
		int days = CalendarUtil.getDays(String.valueOf(key.getDate()).substring(0, 4),
										String.valueOf(key.getDate()).substring(4));
		
		key.setDate(key.getDate() + days);
		
		context.write(key, new DoubleWritable(t1));
	}
}