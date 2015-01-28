package com.hadoop.demo.counter;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.hadoop.demo.surfer.Arith;
import com.hadoop.demo.surfer.CostWritable;

/*
 * IN:
 * 13531490941	( 48851 + 36485 )
 * 13531490942	( 40424	+ 4199 )
 * 
 * OUT:
 * 13531490941	( 48851 + 36485 )	( 48851 + 36485 ) * RADIX
 * 13531490942	( 40424	+ 4199 )	( 40424	+ 4199 ) * RADIX
 * 
 * 
 * */
public class CounterReducer extends Reducer<Text, DoubleWritable, Text, CostWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> value, Context context) throws IOException, InterruptedException {
		double t1 = 0.00d;
		for (DoubleWritable dw : value) {
			t1 = Arith.add(t1, dw.get());
		}

		System.out.print(t1 + " * " + CounterTest.RADIX);
		
		double t2 = Arith.mul(t1, CounterTest.RADIX);

		System.out.println(" = " + t2);
		
		context.write(key, new CostWritable(t1, t2));
	}
}
