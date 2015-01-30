package com.hadoop.demo.expense;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*
 * 求平均值，每月花销
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
 * Frank	Glinda	Joy		Joy		交通		20150101	23409
 * Frank	Sun		Joy		Glinda	聚餐		20150102	26345
 * Frank	Sun		Joy		Sun		把妹		20150117	65046

 * ......
 * 
 * -----------------------
 * OUT:
 * 姓名		年月		当月消费总额		每日花销（平均值）
 * Frank	201502	￥900			￥30
 * 
 * ----------------------
 * 
 * */
public class Runner {
//	private static final String HADOOP_ROOT= "hdfs://hadoop202-master:9000";
	private static final String HADOOP_ROOT= "hdfs://hadoop201:9000";
	
	private static final String PATH_IN_STR = HADOOP_ROOT + "/dd/combine/expense/expense.log";
	private static final String PATH_OUT_STR = HADOOP_ROOT + "/dd/combine/expense/avg";
	
	private static final Path PATH_IN = new Path(PATH_IN_STR);
	private static final Path PATH_OUT = new Path(PATH_OUT_STR);
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI(HADOOP_ROOT), conf);
		if(fs.exists(PATH_OUT)){
			fs.delete(PATH_OUT, true);
			System.out.println("Delete Succ! " + PATH_OUT_STR);
		}
		
		Job job = new Job(conf, Runner.class.getSimpleName());
		
		FileInputFormat.addInputPath(job, PATH_IN);
		FileOutputFormat.setOutputPath(job, PATH_OUT);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombine.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(DetailWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.waitForCompletion(true);
	}
}
