package com.lenss.mstorm.communication.masternode;

import com.google.gson.annotations.Expose;

public class Reply {
	public static final int CLUSTER_ID=0;
	public static final int FAILED=1;
	public static final int TOPOLOGY_ID=2;
	public static final int GETAPK=3;
	public static final int ZOOKEEPERADDR = 4;
	
	@Expose
	private int type=0;
	@Expose
	private String content;
	@Expose
	private String IPAddress;

	public void setType(int _type) {
		type=_type;
	}

	public int getType() {
		return type;
	}

	public void setContent(String _content) {
		content=_content;
	}

	public String getContent() {
		return content;
	}

	public void setAddress(String _address) {
		IPAddress = _address;
	}

	public String getAddress(){
		return IPAddress;
	}

}
