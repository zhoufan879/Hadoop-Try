package com.hadoop.demo.recharge;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PaymentMapper extends Mapper<LongWritable, Text, DetailWritable, TimesWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println(key + " : " + value.toString());
		String[] line = value.toString().split("\t");
		if(line.length > 0 ){
			context.write(
					new DetailWritable(line[0],										// 姓名
							 		   line[1],										// 手机
							 		   Integer.parseInt(line[3].substring(0, 4))),	// 年份
		 		  new TimesWritable(Double.parseDouble(line[2]), 0));	 
		}
	}

}
