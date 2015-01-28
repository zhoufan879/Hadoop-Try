package com.hadoop.demo.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 测试“伪分布模式”
 * 
 * 192.168.1.201
 * 
 * */
public class WordCountTest {

//	private final static String HADOOP_MASTER = "hdfs://hadoop201:9000";
	
	private static final String PATH_IN = "hdfs://hadoop201:9000/dd/hello"; 
	
	private static final String PATH_OUT = "/dd/out";  
	
	
	// Mapper
	/**
	 * KEYIN	即k1		表示行的偏移量
	 * VALUEIN	即v1		表示行文本内容
	 * KEYOUT	即k2		表示行中出现的单词
	 * VALUEOUT	即v2		表示行中出现的单词的次数，固定值1
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

		@Override
		protected void map(LongWritable k1, Text v1, Context context)
				throws IOException, InterruptedException {
			System.out.print("K1: ");
			System.out.println(k1 + " = " + v1);
			
			String[] s = v1.toString().split("\t");
			for(String w : s){
				context.write(new Text(w), new LongWritable(1));
			}
		}
	}
	
	// Reducer
	/**
	 * KEYIN	即k2		表示行中出现的单词
	 * VALUEIN	即v2		表示行中出现的单词的次数
	 * KEYOUT	即k3		表示文本中出现的不同单词
	 * VALUEOUT	即v3		表示文本中出现的不同单词的总次数
	 *
	 */
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		
		@Override
		protected void reduce(Text k2, Iterable<LongWritable> v2, Context context) throws IOException, InterruptedException {
			System.out.print("K2: ");
			System.out.println(k2 + " = " + v2);
			
			long t = 0L;
			for (LongWritable lw : v2) {
				t += lw.get();
			}
			context.write(k2, new LongWritable(t));
		}
	}

	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("mapred.job.tracker","hadoop201:9001");
		
//		JobConf conf = new JobConf();
//		System.out.println("模式：  " + conf.get("mapred.job.tracker"));  
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(new Path(PATH_OUT))){
			fs.delete(new Path(PATH_OUT), true);
		}
		
		Job job = new Job(conf, WordCountTest.class.getSimpleName());
		
//		job.setJarByClass(WordCountTest.class);
		
		job.setMapperClass(MyMapper.class);
//	    job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(PATH_IN));
		
		FileOutputFormat.setOutputPath(job, new Path(PATH_OUT));
		
		job.waitForCompletion(true);
	}
}
