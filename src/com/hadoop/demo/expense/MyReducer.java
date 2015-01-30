package com.hadoop.demo.expense;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.demo.surfer.Arith;

public class MyReducer extends Reducer<DetailWritable, DoubleWritable, DetailWritable, DoubleWritable> {
	@Override
	protected void reduce(DetailWritable key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
		System.out.print("Reducer [ "+ key + " : (");
		
		double t1 = 0.00d;
		for (DoubleWritable dw : value) {
			t1 = Arith.add(t1, dw.get());
			System.out.print(" + " + dw.get());
		}
		System.out.print(" = " + t1);
		
		int days = 1;
		try {
			String s = String.valueOf(key.getDate()).substring(6);
			days = Integer.parseInt(s);
			key.setDate(key.getDate().substring(0, 6));
		} catch (Exception e) {
			days = CalendarUtil.getDays(String.valueOf(key.getDate()).substring(0, 4),
					String.valueOf(key.getDate()).substring(4));
		}
		
		t1 = Arith.div( t1, days, 2);
		
		System.out.println(") / " + days + " = " + t1 + " ]");
		
		context.write(key, new DoubleWritable(t1));
	}
}