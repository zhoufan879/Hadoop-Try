package com.hadoop.demo.rpc.server;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class Start {

	public static void main(String[] args) {
		try {
			
			RPC.getServer(new MyServerImpl(), "127.0.0.1", 12345, new Configuration()).start();
		
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
