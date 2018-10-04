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
	private String myCurrentDynamicNodePath;

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

			// checkServer is up
			String serverName = checkServer();
			boolean exists = false;
			parentStat = zookeeper.exists("/static", false);
			if (parentStat != null) {
				List<String> children = zookeeper.getChildren("/static", false);
				for(String child : children) {
					System.out.println("Child :: " + child);
					exists = child.equals(this.hostname);
					if (exists) {
						createDynamicName(serverName);
						break;
					}

				}
				if (!exists) {
					System.out.println("Exiting :: Static config doesnt exist for the host " + this.hostname);
				}
			}
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;
	}

	private void createDynamicName(String serverName) throws KeeperException {
		try {
			String dynamicServerPath = "/dynamic/" + serverName;
			Stat dynamicNodeStat = zookeeper.exists(dynamicServerPath, false);
			if (dynamicNodeStat != null) {
				String path = zookeeper.create(dynamicServerPath + "/" + this.hostname, null, Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
				if (path != null) {
					String ClientAppPath = zookeeper.create(path + "/G4CMONITOR", null, Ids.OPEN_ACL_UNSAFE,
							CreateMode.EPHEMERAL);
					if (ClientAppPath != null) {
						String ClientQueuePath = ClientAppPath + "/" + "INSTQID";
						Stat nodeStat = zookeeper.exists(ClientQueuePath, false);
						if (nodeStat == null) {

							// get data from static config
							String staticNodePath = "/static/" + this.hostname + "/" + "G4CMONITOR" + "/" + "INSTQID";
							Stat staticNodestat = null;
							byte[] data = zookeeper.getData(staticNodePath, false, staticNodestat);
							String QueuePath;

							QueuePath = zookeeper.create(ClientQueuePath, data, Ids.OPEN_ACL_UNSAFE,
									CreateMode.EPHEMERAL);
							if (QueuePath == null) {
								System.out.println("Failed to create Znode = " + QueuePath);

							} else {
								this.myCurrentDynamicNodePath = QueuePath;
								System.out.println("Dynamic Client Znode:: " + QueuePath + "created successfully");
							}

						}
					} else {
						System.out.println("Failed to create Znode = " + path + "/G4CMONITOR");
					}

				} else {
					System.out.println("Failed to create Znode = " + dynamicServerPath + this.hostname);
				}
			} else {
				System.out.println(" Znode = " + dynamicServerPath + "doesn't exist");
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}

	}

	@Override
	public void run() {
		while (true) {
			if (this.myCurrentDynamicNodePath != null) {
				Stat stat = null;
				byte[] data;
				try {
					data = this.zookeeper.getData(this.myCurrentDynamicNodePath, true, stat);
					System.out.println("Client " + this.hostname + "Processing Queues " + data.toString());
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		}
		// TODO Auto-generated method stub

	}

	public static void main(String argv[]) {

		String url = argv[0];
		ClientAgent client = new ClientAgent(url);
		Thread t = new Thread(client);
		t.start();

	}
}
