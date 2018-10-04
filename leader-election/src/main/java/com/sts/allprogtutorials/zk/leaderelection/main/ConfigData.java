package com.sts.allprogtutorials.zk.leaderelection.main;

import java.util.List;

import org.apache.zookeeper.data.Stat;

public class ConfigData {
	
	int waitTime;
	List<String> queueIds;
	int maxQueueSize ;
	String znodePath;
	Stat stat;
	
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
	
	@Override
	public String toString() {
		return "ConfigData [waitTime=" + waitTime + ", queueIds=" + queueIds + ", maxQueueSize=" + maxQueueSize
				+ ", znodePath=" + znodePath + ", stat=" + stat + "]";
	}

}
