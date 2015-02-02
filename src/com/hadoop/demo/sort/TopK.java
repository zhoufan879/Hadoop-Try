package com.hadoop.demo.sort;

import java.io.IOException;
import java.net.URI;
import java.util.Comparator;
import java.util.SortedMap;
import java.util.TreeMap;

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

/*
 * 测试“集群模式”
 * 
 * hadoop202-master
 * hadoop203-slave1
 * hadoop204-slave2
 * -----------------------
 * 
 * IN:
 * 编号		姓名		语		数		外
 * 1001		Frank	100		100		100
 * ......
 * 
 * -----------------------
 * OUT:
 * Top 5
 * 姓名 		总分
 * Frank	300
 * Joy		299
 * Belle	298
 * 
 * ----------------------
 * 
 * */
public class TopK {

	private final static String HADOOP_MASTER = "hdfs://hadoop202-master:9000";
	
	private static final String PATH_IN = HADOOP_MASTER + "/dd/topk/exam.txt"; 
	
	private static final String PATH_OUT = HADOOP_MASTER + "/dd/topk/out";  

	public static class MyMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

		@Override
		protected void map(LongWritable k1, Text v1, Context context)
				throws IOException, InterruptedException {
			System.out.println(k1 + " = " + v1);
			
			String[] s = v1.toString().split("\t");
			
			double t = Arith.add(new Double(s[2]), new Double(s[3]), new Double(s[4]));
			
			context.write(new Text(s[0] + "|" + s[1]), new DoubleWritable(t));
		}
	}
	
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private SortedMap<Double, Text> top5 = new TreeMap<Double, Text>(new CollatorComparator());
		
		@Override
		protected void reduce(Text k2, Iterable<DoubleWritable> v2, Context context) throws IOException, InterruptedException {
			System.out.print(k2 + " : " );
			
			DoubleWritable v = new DoubleWritable(0.00d);
			for (DoubleWritable dw : v2) {
				v = dw;
				System.out.print(dw.get() + " ");
			}
			System.out.println();
			
			top5.put(v.get(), new Text(k2.toString()));
			if(top5.size()>5){
				top5.remove(top5.lastKey());
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println("\nClearUp....... ");
			for(Double t : top5.keySet()) {
				System.out.println(top5.get(t) + ":" + t);
				context.write(top5.get(t), new DoubleWritable(t));
			}
		}
	}
	
	public static class CollatorComparator implements Comparator<Double> {

		@Override
		public int compare(Double o1, Double o2) {
			return -o1.compareTo(o2);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();

		FileSystem fs = FileSystem.get(new URI(HADOOP_MASTER), conf);
		if(fs.exists(new Path(PATH_OUT))){
			fs.delete(new Path(PATH_OUT), true);
			System.out.println("delete file: [" + PATH_OUT + "]");
		}
		
		Job job = new Job(conf, TopK.class.getSimpleName());
		
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
