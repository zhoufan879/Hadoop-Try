package com.hadoop.demo.surfer;

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
 * ----------------------
 * 
 * 
 * */
public class Phone {

	private final static String HADOOP_MASTER = "hdfs://hadoop202-master:9000";
	
	private static final String PATH_IN = HADOOP_MASTER + "/dd/phone/phone.txt"; 
	
	private static final String PATH_OUT = HADOOP_MASTER + "/dd/phone/out";  
	
	private static final double RADIX = 0.10d;  
	
	// Mapper
	/**
	 * KEYIN	即k1		表示行的偏移量
	 * VALUEIN	即v1		表示行文本内容
	 * KEYOUT	即k2		帐号
	 * VALUEOUT	即v2		总流量（ 上行 + 下行 ） 
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

		@Override
		protected void map(LongWritable k1, Text v1, Context context)
				throws IOException, InterruptedException {
			System.out.println(k1 + " = " + v1);
			/*
				K1: 0 = 13531490941	13531490941	FZ-JW-LB-OB-OO-ON:CMCC	123.235.226.221	www.1.com	20968	27461	200
			 * */
			
			String[] s = v1.toString().split("\t");
			double t = Arith.add(new Double(s[5]), new Double(s[6]));
			
			context.write(new Text(s[1]), new DoubleWritable(t));
		}
	}
	
	// Reducer
	/**
	 * KEYIN	即k2		帐号
	 * VALUEIN	即v2		总流量（组）
	 * KEYOUT	即k3		帐号
	 * VALUEOUT	即v3		总流量	费用
	 *
	 */
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, CostWritable> {
		
		@Override
		protected void reduce(Text k2, Iterable<DoubleWritable> v2, Context context) throws IOException, InterruptedException {
			System.out.print(k2 + " : " );
			/*
				K2: 13531490941	48429[,...]
			 **/
			
			double t1 = 0.00d;
			for (DoubleWritable dw : v2) {
				t1 = Arith.add(t1, dw.get());
			}

			System.out.print(t1 + " * " + RADIX);
			
			// 计费
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
		
		Job job = new Job(conf, Phone.class.getSimpleName());
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(PATH_IN));
		FileOutputFormat.setOutputPath(job, new Path(PATH_OUT));
		
		job.waitForCompletion(true);
	}
}
