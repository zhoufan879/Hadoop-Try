package com.hadoop.demo.partitioner;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class MyPartitioner extends HashPartitioner<Text, DoubleWritable> {
	@Override
	public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
		int l = key.toString().length();
		
		if( l == 11 ){						// 手机
			return 1 % numReduceTasks;
		} else if ( l == 3) {				// 短号
			return 2 % numReduceTasks;
		} else {							// 其它（电话）
			return 3 % numReduceTasks;		
		}
	}
}
