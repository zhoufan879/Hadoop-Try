package com.hadoop.demo.rpc.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.hadoop.demo.rpc.server.IMyServer;

/*
 * 使用 RPC 操作 HDFS 
 * 
 * 
 * */
public class HDFSRPCTest {

	public static void main(String[] args) {
		
		try {
			IMyServer ms = (IMyServer) RPC.waitForProxy(IMyServer.class, IMyServer.VERSION, new InetSocketAddress("127.0.0.1", 12345), new Configuration());
			
			String re = ms.hello("world");
			
			System.out.println(re);	// hello world
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
