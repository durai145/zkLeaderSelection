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

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sts.allprogtutorials.zk.leaderelection.main.ConfigData.zNodeInfo;

public class ClientAgent implements Runnable {
	ZooKeeper zookeeper;
	String hostname;
	private static final String ELECTED_SERVER_PATH = "/election/server";
	private static final String CLIENT_APP_NAME = "G4CMONITOR";
	private String myCurrentDataNodePath;
	static Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

	public ClientAgent(String url) {
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			this.hostname = ip.getHostName();
			System.out.println("hostname :: " + this.hostname);
			try {
				    zookeeper = new ZooKeeper(url, 3000, null);
				    System.out.println("Zookeper connection " + zookeeper);   
				    findAndCreateZnode();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (UnknownHostException e) {
			System.out.println("Unable to fetch hostname for the current Client system");
			e.printStackTrace();

		}
	}

	public boolean findAndCreateZnode() {
		Stat parentStat;
		boolean exists = false;
		try {

			// checkServer is up
			String serverName = checkServer();
			
			parentStat = zookeeper.exists("/static", false);
			if (parentStat != null) {
				List<String> children = zookeeper.getChildren("/static", false);
				for (String child : children) {
					System.out.println("Child :: " + child);
					exists = child.equals(this.hostname);
					if (exists) {
						System.out.println("Found the node with hostname:: " + this.hostname);
						createDataNode(serverName);
						break;
					}

				}
				if (!exists) {
					System.out.println("Exiting :: Static config doesnt exist for the host " + this.hostname);
				}else {
					byte[] data = null;
					//now we need to create a dynamic node for this host for the server to watch
					zookeeper.create("/dynamic" + "/" + this.hostname, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				}
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new IllegalStateException(
					"ClientAgent::findAndCreateZnode error while creating dynamic config from static config for clientNodes"
							+ e);
		}

		return exists;
	}
    
	private void createDataNode(String serverName) throws KeeperException {
		try {
			zNodeInfo staticNodePath = new ConfigData.zNodeInfo("static", this.hostname, CLIENT_APP_NAME);
			ConfigData staticConfig = readClientData(staticNodePath.getPath());
			if (staticConfig != null)
			{			    
			    Stat dataNodeStat = zookeeper.exists(staticNodePath.getDataPath(), false);
			    if (dataNodeStat == null) {
				    String path = zookeeper.create(staticNodePath.getDataPath(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				    if (path != null) {
					   String ClientAppPath = zookeeper.create(path + this.hostname, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
					if (ClientAppPath != null) {
						this.myCurrentDataNodePath = ClientAppPath;
						}
					} else {
						System.out.println("Failed to create Znode = " + path + CLIENT_APP_NAME);
					}

				} else {
					System.out.println("Failed to create Znode = " + staticNodePath.getDataPath());
				}
			} else {
				System.out.println(" No Static Configuration available for the client " + staticNodePath.getDataPath());
			}
		
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException(
					"ClientAgent::createDynamicName:: Exception while creating Dynamic zNode under" + serverName);

		}

	}

	private String checkServer() throws KeeperException, InterruptedException {

		byte[] serverName = null;
		Stat serverStat;
		try {
			serverStat = zookeeper.exists(ELECTED_SERVER_PATH, false);
			if (serverStat != null) {
				serverName = zookeeper.getData(ELECTED_SERVER_PATH, true, serverStat);
			}

			return new String(serverName);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException(
					"ClientAgent::checkServer:: Exception while getting data for zNode" + ELECTED_SERVER_PATH + e);
		}

	}

	@Override
	public void run() {
		while (true) {
			if (this.myCurrentDataNodePath != null) {
				System.out.println("myCurrentDataNodePath" + myCurrentDataNodePath);
				try {
					int waitTime = 5;
					ConfigData clientData = readClientData(this.myCurrentDataNodePath);
					if (clientData != null) {
						waitTime = clientData.getWaitTime();
						System.out.println("Client " + this.hostname + "Processing Queues " + clientData.getQueueIds());
					}
					wait(waitTime);
				} catch (KeeperException | InterruptedException e) {
					throw new IllegalStateException(
							"Exception in run:: unable to getData for " + this.myCurrentDataNodePath);
				}

			}

		}

	}

	public ConfigData readClientData(String dataClientPath) throws KeeperException, InterruptedException {
		byte[] data;

		try {
			data = zookeeper.getData(dataClientPath, false, null);
			String strData = new String(data);
			ConfigData nodeConfigData = gson.fromJson(strData, ConfigData.class);
			return nodeConfigData;
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException("Exception in readDataFromNode::  " + e);
		}

	}

	public static void main(String argv[]) {
		System.out.println("Inside Main::ClientAgent ");
		if (argv.length < 1) {
			usage();
		} else {
			String url = argv[0];
			ClientAgent client = new ClientAgent(url);
			Thread t = new Thread(client);
			t.start();
		}

	}

	private static void usage() {
		System.out.println("ClientAgent <IPaddress:port list> ");

	}
}
