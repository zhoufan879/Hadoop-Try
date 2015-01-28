package com.hadoop.demo.java;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

public class HDFSTest {

	private final static String HADOOP_201_PATH = "hdfs://192.168.1.201:9000/dd/hello";
	
	public static void main(String[] args) {
		
		try {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			URL url = new URL(HADOOP_201_PATH);
			InputStream in = url.openStream();
			IOUtils.copyBytes(in, System.out, 1024, true);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
}
