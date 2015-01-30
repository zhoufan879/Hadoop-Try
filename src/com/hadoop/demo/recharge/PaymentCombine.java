package com.hadoop.demo.recharge;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.demo.surfer.Arith;

public class PaymentCombine extends Reducer<DetailWritable, TimesWritable, DetailWritable, TimesWritable> {
	@Override
	protected void reduce(DetailWritable key, Iterable<TimesWritable> value, Context context) throws IOException, InterruptedException {
		System.out.print("Combine [ "+ key + " :");
		double t1 = 0.00d;
		int times = 0;
		for (TimesWritable dw : value) {
			t1 = Arith.add(t1, dw.getRmb());
			times++;
			System.out.print(" + "+ dw.getRmb());
		}
		
		System.out.println(" | " + times + " ]");
		
		 context.write(key, new TimesWritable(t1, times));
	}
}