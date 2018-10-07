package com.sts.allprogtutorials.zk.leaderelection.main;

import java.util.List;

import org.apache.zookeeper.data.Stat;

public class ConfigData {
	
	int waitTime;
	List<String> queueIds;
	int maxQueueSize;
	String znodePath;
	Stat stat;
	transient zNodeInfo znode;
	
	public void setZnode(zNodeInfo znode) {
		this.znode = znode;
	}
	public zNodeInfo getZnode() {
		return znode;
	}
	public Stat getStat() {
		return stat;
	}
	public void setStat(Stat stat) {
		this.stat = stat;
	}
	public String getZnodePath() {
		return znodePath;
	}
	public void setZnodePath(String znodePath) {
		this.znodePath = znodePath;
		System.out.println("znodePath:: " + znodePath );
		if(znodePath != null)
		{
		   //String[] nodes = znodePath;
		   znode= new zNodeInfo(znodePath);
		}
	}
	public int getWaitTime() {
		return waitTime;
	}
	public void setWaitTime(int waitTime) {
		this.waitTime = waitTime;
	}
	public List<String> getQueueIds() {
		return queueIds;
	}
	public void setQueueIds(List<String> queueIds) {
		this.queueIds = queueIds;
	}
	public int getMaxQueueSize() {
		return maxQueueSize;
	}
	
	public void setMaxQueueSize(int maxQueueSize) {
		this.maxQueueSize = maxQueueSize;
	}
	
	public static class zNodeInfo {
		public zNodeInfo() {}
		public zNodeInfo(String type, String app, String host) {
			this.app = app;
			this.host = host;
			this.type = type;
		}
		String app;
		String host;
		String type;
		
		
		public String getApp() {
			return app;
		}
		public void setApp(String app) {
			this.app = app;
		}
		public String getHost() {
			return host;
		}
		public void setHost(String host) {
			this.host = host;
		}
		public String getType() {
			return type;
		}
		public void setType(String type) {
			this.type = type;
		}
		
		
		public zNodeInfo(String znodePath) {
			String[] nodes = znodePath.split("/");
			this.app = nodes[2];
			this.host = nodes[3];
			this.type = nodes[1];
			
		}
		
		public String getPath() {
			return "/" + this.type + "/" + this.app + "/" + this.host;
		}
		
		public String getStaticPath() {
			return "/static" + "/" + this.app + "/" + this.host;
		}
		public String getDataPath() {
			return "/data" + "/" + this.app + "/" + this.host;
		}
		public String getDynamicPath() {
			return "/dynamic" + "/" + this.app + "/" + this.host;
		}
		@Override
		public String toString() {
			return getPath(); 
		}
		
	}
	
	
	@Override
	public String toString() {
		return "ConfigData [waitTime=" + waitTime + ", queueIds=" + queueIds + ", maxQueueSize=" + maxQueueSize
				+ ", znodePath=" + znodePath + ", stat=" + stat + "]";
	}

}
