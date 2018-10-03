package com.sts.allprogtutorials.zk.leaderelection.main;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class ClientAgent implements Runnable {
	ZooKeeper zookeeper;
	String hostname;
	private static final String ELECTED_SERVER_PATH = "/election/server";

	public ClientAgent(String url) {
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			this.hostname = ip.getHostName();
			try {
				zookeeper = new ZooKeeper(url, 3000, null);
				findAndCreateZnode();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public boolean findAndCreateZnode() {
		Stat parentStat;
		try {
			
			//checkServer is up 
			String serverName=checkServer() ;
			boolean exists = false;
			parentStat = zookeeper.exists("/static", false);
			if (parentStat != null) {
				List<String> children = zookeeper.getChildren("/static", false);
				children.forEach(child -> {
					 exists = child.equals(this.hostname);
					if (exists) {
						createDynamicName(serverName);
						break;
						}
							
				});
				if(!exists)
				{
					System.out.println("Exiting :: Static config doesnt exist for the host " + this.hostname);
				}
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private void createDynamicName(String serverName) {
		String dynamicServerPath = "/dynamic/"+serverName;
		Stat dynamicNodeStat = zookeeper.exists(dynamicServerPath, false);
		if (dynamicNodeStat != null) {
			String path = zookeeper.create(dynamicServerPath+this.hostname, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			if (path != null) {
			    String ClientAppPath = zookeeper.create(path+, data, acl, createMode) 
			}
		}
		
	}

	private String checkServer() {
		
		byte[] serverName;
		Stat serverStat = zookeeper.exists(ELECTED_SERVER_PATH, true);
		if (serverStat != null)
		{
			serverName = zookeeper.getData(ELECTED_SERVER_PATH, true, serverStat);
		}
		if (serverName != null)
		return serverName.toString();
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub

	}
	
	public static void main(String argv[]) {
		
		String url = argv[0];
		ClientAgent client = new ClientAgent(url);
		client.Start();
		
	}
	}

}
