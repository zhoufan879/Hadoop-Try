package com.hadoop.demo.combine;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.demo.surfer.Arith;

public class MyCombine extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
		System.out.print("MyCombine [ "+ key + " :");
		double t1 = 0.00d;
		for (DoubleWritable dw : value) {
			t1 = Arith.add(t1, dw.get());
			System.out.print(" "+ dw.get());
		}
		
		System.out.println(" ]");
		
		context.write(key, new DoubleWritable(t1));
	}
}
