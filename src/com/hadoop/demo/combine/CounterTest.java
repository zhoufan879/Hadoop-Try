package com.hadoop.demo.combine;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.demo.counter.CounterMapper;
import com.hadoop.demo.counter.CounterReducer;


/*
 * 测试“集群模式”
 * 
 * hadoop202-master
 * hadoop203-slave1
 * hadoop204-slave2
 * -----------------------
 * 
 * IN:
 * 13531490941	13531490941	FZ-JW-LB-OB-OO-ON:CMCC	123.235.226.221	www.1.com	20968	27461	200
 * ......
 * 
 * -----------------------
 * OUT:
 * 帐号			总流量（KB）						总费用 （假设0.1元/KB）
 * 13531490941	( 20968 + 27461 = 48429)	48429 * 0.1￥ = 4842.9
 * 
 * AFFIX:
 * 某用户访问次数
 * 18621588915	count(*)
 * 
 * AFFIX:
 * 网站访问正常率
 * 200/all		
 * 
 * ----------------------
 * 
 * */
public class CounterTest {
	private static final String HADOOP_ROOT= "hdfs://hadoop202-master:9000";
	
	private static final String PATH_IN_STR = HADOOP_ROOT + "/dd/combine/phone.txt";
	private static final String PATH_OUT_STR = HADOOP_ROOT + "/dd/combine/out";
	
	private static final Path PATH_IN = new Path(PATH_IN_STR);
	private static final Path PATH_OUT = new Path(PATH_OUT_STR);
	
	public static final String VIP_ACCOUNT = "18621588915"; 
	
	public static final String STATE_SUCC = "200"; 

	public static final double RADIX = 0.15d;  
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(new URI(HADOOP_ROOT), conf);
		if(fs.exists(PATH_OUT)){
			fs.delete(PATH_OUT, true);
			System.out.println("Delete Succ! " + PATH_OUT_STR);
		}
		
		Job job = new Job(conf, CounterTest.class.getSimpleName());
		
		FileInputFormat.addInputPath(job, PATH_IN);
		FileOutputFormat.setOutputPath(job, PATH_OUT);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(CounterMapper.class);
		job.setCombinerClass(MyCombine.class);
		job.setReducerClass(CounterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.waitForCompletion(true);
	}
}
