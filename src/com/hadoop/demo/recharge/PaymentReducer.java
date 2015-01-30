package com.hadoop.demo.recharge;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.demo.surfer.Arith;

public class PaymentReducer extends Reducer<DetailWritable, TimesWritable, DetailWritable, DoubleWritable> {
	@Override
	protected void reduce(DetailWritable key, Iterable<TimesWritable> value, Context context) throws IOException, InterruptedException {
		System.out.print("Reducer [ "+ key + " :");
		
		double t1 = 0.00d;
		for (TimesWritable dw : value) {
			t1 = Arith.div( dw.getRmb(), dw.getTimes(), 2);
			System.out.print(" "+ dw.getRmb() + "/" + dw.getTimes() + " = " + t1);
		}
		System.out.println(" ]");
		
		context.write(key, new DoubleWritable(t1));
	}
}