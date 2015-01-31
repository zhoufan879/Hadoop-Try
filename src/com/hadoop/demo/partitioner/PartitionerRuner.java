package com.hadoop.demo.partitioner;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.demo.surfer.Arith;
import com.hadoop.demo.surfer.CostWritable;

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
 * 18593726191	8482713		KI-AX-QA-BR-VT-JG:CMCC	139.210.225.211	www.8.com	3464	39336	200
 * ......
 * 
 * 
 * -----------------------
 * 
 * OUT
 * File 1:
 * 手机			总流量（KB）						总费用 （假设0.1元/KB）
 * 13531490941	( 20968 + 27461 = 48429)		48429 * 0.1￥ = 4842.9
 * 
 * 
 * File 2:
 * 电话			总流量（KB）						总费用 （假设0.1元/KB）
 * 84224112		( 20968 + 27461 = 48429)		48429 * 0.1￥ = 4842.9
 *
 *
 * File 3:
 * 短号			总流量（KB）						总费用 （假设0.1元/KB）
 * 563			( 20968 + 27461 = 48429)		48429 * 0.1￥ = 4842.9
 *
 * 
 * */
public class PartitionerRuner {

	private final static String HADOOP_MASTER = "hdfs://hadoop202-master:9000";
	
	private static final String PATH_IN = HADOOP_MASTER + "/dd/partitioner/phone2.txt"; 
	
	private static final String PATH_OUT = HADOOP_MASTER + "/dd/partitioner/out";  
	
	private static final double RADIX = 0.12d;  
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{
		@Override
		protected void map(LongWritable k1, Text v1, Context context)
				throws IOException, InterruptedException {
			System.out.println(k1 + " = " + v1);
			String[] s = v1.toString().split("\t");
			double t = Arith.add(new Double(s[5]), new Double(s[6]));
			context.write(new Text(s[1]), new DoubleWritable(t));
		}
	}

	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, CostWritable> {
		@Override
		protected void reduce(Text k2, Iterable<DoubleWritable> v2, Context context) throws IOException, InterruptedException {
			System.out.print(k2 + " : " );
			
			double t1 = 0.00d;
			for (DoubleWritable dw : v2) {
				t1 = Arith.add(t1, dw.get());
			}

			System.out.print(t1 + " * " + RADIX);
			
			double t2 = Arith.mul(t1, RADIX);

			System.out.println(" = " + t2);
			
			context.write(k2, new CostWritable(t1, t2));
		}
	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI(HADOOP_MASTER), conf);
		if(fs.exists(new Path(PATH_OUT))){
			fs.delete(new Path(PATH_OUT), true);
			System.out.println("delete file: [" + PATH_OUT + "]");
		}
		Job job = new Job(conf, PartitionerRuner.class.getSimpleName());
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(PATH_IN));
		FileOutputFormat.setOutputPath(job, new Path(PATH_OUT));
		
		// 必须打成jar 包执行 
		job.setJarByClass(PartitionerRuner.class);
		
		// 指定分区类
		job.setPartitionerClass(MyPartitioner.class);
		
		// 设置分区数
		job.setNumReduceTasks(3);
		
		job.waitForCompletion(true);
	}
}
