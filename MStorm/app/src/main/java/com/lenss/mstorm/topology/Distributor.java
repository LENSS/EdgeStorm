package com.lenss.mstorm.topology;

import android.net.LocalSocket;
import android.net.LocalSocketAddress;

import com.lenss.mstorm.communication.internodes.MessageQueues;
import com.lenss.mstorm.core.ComputingNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;

public abstract class Distributor extends BTask {

	@Override
	public void prepare() { }

	@Override
	public void execute() { }

	@Override
	public void postExecute() { }

}
