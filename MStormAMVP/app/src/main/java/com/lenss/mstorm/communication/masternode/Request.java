package com.lenss.mstorm.communication.masternode;

import java.io.Serializable;

import com.google.gson.annotations.Expose;

public class Request implements Serializable{
	public static final int JOIN=1;
	public static final int TOPOLOGY=2;
	public static final int GETAPKFILE=3;
	public static final int PHONESTATUS=4;
	public static final int CANCEL = 5;
	public static final int GETZOOKEEPER = 6;

	@Expose
	private int type;
	@Expose
	private String fileName;
	@Expose
	private String content;
	@Expose
	private String cluster_id;
	@Expose
	private String IP;
	@Expose
	private String GUID;

	public void setIP(String IP){
		this.IP=IP;
	}

	public String getIP(){
		return IP;
	}

	public void setReqType(int _type){
		type=_type;
	}

	public int getReqType(){
		return type;
	}

	public void setClusterID(String _cluster_id){
		cluster_id=_cluster_id;
	}

	public String getClusterID() {
		return cluster_id;
	}

	public void setFileName(String fileName){
		this.fileName = fileName;
	}

	public String getFileName(){
		return fileName;
	}

	public void setContent(String _content){
		content=_content;
	}

	public String getContent(){
		return content;
	}

	public void setGUID(String GUID){this.GUID=GUID;}

	public String getGUID(){return GUID;}
}
