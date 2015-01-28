package com.hadoop.demo.rpc.server;

import java.io.IOException;

public class MyServerImpl implements IMyServer {

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return IMyServer.VERSION;
	}

	@Override
	public String hello(String w) {
		return "hello " + w;
	}
	

}
