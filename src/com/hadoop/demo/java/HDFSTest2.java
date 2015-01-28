package com.hadoop.demo.java;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/*
 * 使用 Java 操作 HDFS 
 * 
 * 1. 创建目录 
 * 
 * 2. 上传文件
 * 
 * 3. 下载文件
 * 
 * 4. 删除文件（夹）
 * 
 * */
public class HDFSTest2 {

	private final static String HADOOP_MASTER = "hdfs://hadoop202-master:9000";

	private final static String DIR_JAVA = "/java";
	
	private final static String DIR_JAVA_CLASS = "F:\\Eclipse\\eclipse_workspace\\HadoopOps\\src\\com\\hadoop\\demo\\HDFSTest2.java";
	
	private final static String DIR_OUT = "F:\\";
	
	public static void main(String[] args) {
		
		try {
			FileSystem fs = FileSystem.get(new URI(HADOOP_MASTER), new Configuration());
			
			// 1. 创建目录 
			fs.mkdirs(new Path(DIR_JAVA));
			
			// 2. 上传文件			
			fs.copyFromLocalFile(new Path(DIR_JAVA_CLASS), new Path(DIR_JAVA));
			
			// 3. 下载文件
			fs.copyToLocalFile(new Path(DIR_JAVA), new Path(DIR_OUT));
			
			// 4. 删除文件（夹）
			fs.delete(new Path(DIR_JAVA), true);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
