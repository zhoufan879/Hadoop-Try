package com.hadoop.demo.expense;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, DetailWritable, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		System.out.println(key + " : " + value.toString());
		String[] line = value.toString().split("\t");
		if(line.length > 0 ){
			context.write(
					new DetailWritable(line[0],										// 姓名
							 		   line[5].substring(0, 6)),					// 年月
							 	new DoubleWritable(Double.parseDouble(line[6])));	// 金额	 
		}
	}

}
