package com.hadoop.demo.recharge;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*
 * 求平均值，每年资助联通多少RMB
 * 
 * rework  
 * 
 * Q&A 
 * 1. K/V 如何传递 JavaBean 或多个值 
 * 
 * 
 * -----------------------
 * 测试“集群模式”
 * 
 * hadoop202-master
 * hadoop203-slave1
 * hadoop204-slave2
 * -----------------------
 * 
 * IN:
 * Frank	18621588915	100	20150129
 * ......
 * 
 * -----------------------
 * OUT:
 * 手机号		姓名		年份		平均每年消费金额		
 * 18621588915	Frank	2015	103982
 * 
 * ----------------------
 * 
 * */
public class PaymentTest {
	private static final String HADOOP_ROOT= "hdfs://hadoop202-master:9000";
	
	private static final String PATH_IN_STR = HADOOP_ROOT + "/dd/combine/recharge/history.log";
	private static final String PATH_OUT_STR = HADOOP_ROOT + "/dd/combine/recharge/avg";
	
	private static final Path PATH_IN = new Path(PATH_IN_STR);
	private static final Path PATH_OUT = new Path(PATH_OUT_STR);
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI(HADOOP_ROOT), conf);
		if(fs.exists(PATH_OUT)){
			fs.delete(PATH_OUT, true);
			System.out.println("Delete Succ! " + PATH_OUT_STR);
		}
		
		Job job = new Job(conf, PaymentTest.class.getSimpleName());
		
		FileInputFormat.addInputPath(job, PATH_IN);
		FileOutputFormat.setOutputPath(job, PATH_OUT);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(PaymentMapper.class);
		job.setCombinerClass(PaymentCombine.class);
		job.setReducerClass(PaymentReducer.class);
		
		job.setOutputKeyClass(DetailWritable.class);
		job.setOutputValueClass(TimesWritable.class);
		
		job.waitForCompletion(true);
	}
}
