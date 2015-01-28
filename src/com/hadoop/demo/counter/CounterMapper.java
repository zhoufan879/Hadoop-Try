package com.hadoop.demo.counter;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import com.hadoop.demo.surfer.Arith;

/*
 * IN:
 * 0 = 13531490941	13531490941	FZ-JW-LB-OB-OO-ON:CMCC	123.235.226.221	www.1.com	48851	36485	200
 * 89 = 13531490942	13531490942	WC-AU-CL-KJ-FI-NQ:CMCC	139.199.183.44	www.2.com	40424	4199	300
 * 
 * OUT:
 * 13531490941	( 48851 + 36485 )
 * 13531490942	( 40424	+ 4199 )
 * 
 * COUNTER:
 * 某用户访问次数
 * 18621588915	count(*)
 * 
 * COUNTER:
 * 网站正常访问次数
 * 200
 * 
 * 
 * */
public class CounterMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Counter vipCounter = context.getCounter("VIP", CounterTest.VIP_ACCOUNT);
		Counter sucCounter = context.getCounter("200", CounterTest.STATE_SUCC);
		
		String[] line = value.toString().split("\t");
		if(line[0].contains(CounterTest.VIP_ACCOUNT)){
			vipCounter.increment(1L);
		}
		
		if(line[7].equals(CounterTest.STATE_SUCC)){
			sucCounter.increment(1L);
		}
		
		double t = Arith.add(new Double(line[5]), new Double(line[6]));
		
		context.write(new Text(line[1]), new DoubleWritable(t));
	}

}
