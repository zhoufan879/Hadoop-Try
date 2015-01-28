package com.hadoop.demo.rpc.server;

import org.apache.hadoop.ipc.VersionedProtocol;

public interface IMyServer extends VersionedProtocol {

	final long VERSION = 22222L;
	
	String hello( String w );
	
}
