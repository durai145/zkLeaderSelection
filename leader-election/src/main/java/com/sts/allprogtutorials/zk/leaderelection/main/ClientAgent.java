package com.sts.allprogtutorials.zk.leaderelection.main;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
import com.sts.allprogtutorials.zk.leaderelection.nodes.ProcessNode.ProcessNodeWatcher;
import com.sts.allprogtutorials.zk.utils.ZooKeeperService;

public class ClientAgent implements Runnable {
	ZooKeeper zookeeper;
	String hostname;
	private static final String ELECTED_SERVER_PATH = "/election/server";
	private static final String CLIENT_APP_NAME = "G4CMONITOR";
	private String staticMyCurrentNodePath;
	// private ZooKeeperService zooKeeperService;
	static Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

	public ClientAgent(String url) {
		InetAddress ip;
		try {
			ip = InetAddress.getLocalHost();
			this.hostname = ip.getHostName();
			System.out.println("hostname :: " + this.hostname);
			this.staticMyCurrentNodePath = new zNodeInfo("/data/G4CMONITOR/"+this.hostname).getDataPath();
			try {
				// zooKeeperService = new ZooKeeperService(url, null);
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

			parentStat = zookeeper.exists("/static/G4CMONITOR", false);
			if (parentStat != null) {
				List<String> children = zookeeper.getChildren("/static/G4CMONITOR", false);
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
				} else {
					byte[] data = null;
					// now we need to create a dynamic node for this host for the server to watch
					zookeeper.create("/dynamic" + "/" + this.hostname, data, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				}
			}
		} catch (KeeperException e) { // TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return exists;
	}

	private void createDataNode(String serverName) throws KeeperException {
		try {
			zNodeInfo staticNodePath = new ConfigData.zNodeInfo("static", CLIENT_APP_NAME, this.hostname);
			ConfigData staticConfig = readClientData(staticNodePath.getStaticPath());
			if (staticConfig != null) {
				Stat dataNodeStat = zookeeper.exists(staticNodePath.getDynamicPath(), false);
				if (dataNodeStat == null) {

					List<String> allNodePath = parseZNodePath(staticNodePath.getDynamicPath());
					// checkNode is exit
					System.out.println("allNodePath Before removing " + allNodePath);
					allNodePath.remove(allNodePath.size() - 1);
					System.out.println("allNodePath After removing " + allNodePath);
					allNodePath.forEach(nodeItem -> {

						checkZNodeORCreate(nodeItem);
					});
					Stat nodeStat = zookeeper.exists(staticNodePath.getDynamicPath(), true);
					if (nodeStat == null) {
						String path = zookeeper.create(staticNodePath.getDynamicPath(), null, Ids.OPEN_ACL_UNSAFE,
								CreateMode.EPHEMERAL);

						if (path != null) {
							System.out.println("Created Znode successfully :: " + path);
						} else {
							System.out.println("Failed to create Znode = " + staticNodePath.getDataPath());
						}
					} else {
						System.out.println("Node already exists");
					}
				} else {
					System.out.println(
							" No Static Configuration available for the client " + staticNodePath.getDataPath());
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException(
					"ClientAgent::createDynamicName:: Exception while creating Dynamic zNode under" + serverName);

		}

	}

	public List<String> parseZNodePath(String zNodPath) {
		List<String> nodes = new ArrayList<>();
		String[] dirs = zNodPath.split("/");
		String parent = "";
		for (String dir : dirs) {
			if (dir != null && !dir.isEmpty()) {
				parent = parent + "/" + dir;
				nodes.add(parent);
			}
		}
		return nodes;
	}

	public void checkZNodeORCreate(String node) {
		try {
			System.out.println("checkZNodeORCreate::Node :: " + node);
			Stat nodeStat = zookeeper.exists(node, false);
			System.out.println("Nodestat :: " + nodeStat);
			if (nodeStat == null) {
				String nodePath = zookeeper.create(node, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				System.out.println("NodePath Created = " + nodePath);
			}

		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private String checkServer() {

		byte[] serverName = null;
		Stat serverStat;
		try {
			serverStat = zookeeper.exists(ELECTED_SERVER_PATH, false);
			if (serverStat != null) {
				serverName = zookeeper.getData(ELECTED_SERVER_PATH, true, serverStat);
			}

		} catch (KeeperException | InterruptedException e) {
			System.out.println(e);
			e.printStackTrace();
			// throw new IllegalStateException(
			// "ClientAgent::checkServer:: Exception while getting data for zNode" +
			// ELECTED_SERVER_PATH + e);
		}
		return new String(serverName);

	}

	@Override
	public void run() {
		System.out.println("myCurrentDataNodePath ::   " + staticMyCurrentNodePath);
		while (true) {
			if (this.staticMyCurrentNodePath != null) {
				System.out.println("myCurrentDataNodePath" + staticMyCurrentNodePath);
				try {
					int waitTime = 5;
					ConfigData clientData = readClientData(this.staticMyCurrentNodePath);
					if (clientData != null) {
						waitTime = clientData.getWaitTime();
						System.out.println("Client " + this.hostname + "Processing Queues " + clientData.getQueueIds());
					}
					Thread.sleep(waitTime*1000);
				} catch (KeeperException | InterruptedException e) {
					throw new IllegalStateException(
							"Exception in run:: unable to getData for " + this.staticMyCurrentNodePath);
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
			System.out.println("Launching Thread ClientAgent");
			Thread t = new Thread(client);
			try {
				Thread.sleep(20000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			t.start();
			System.out.println("Thread Invoked");
		}

	}

	private static void usage() {
		System.out.println("ClientAgent <IPaddress:port list> ");

	}
}
