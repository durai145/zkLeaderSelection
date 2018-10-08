/**
 * 
 */
package com.sts.allprogtutorials.zk.leaderelection.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sts.allprogtutorials.zk.leaderelection.main.ConfigData;
import com.sts.allprogtutorials.zk.leaderelection.main.ConfigData.zNodeInfo;
import com.sts.allprogtutorials.zk.utils.ZooKeeperService;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author Sain Technology Solutions
 *
 */
public class ProcessNode implements Runnable {

	private static final Logger LOG = Logger.getLogger(ProcessNode.class);

	private static final String LEADER_ELECTION_ROOT_NODE = "/election";
	private static final String PROCESS_NODE_PREFIX = "/p_";
	private static final String ELECTED_SERVER_LEADER_NODE_PATH = "/election/server";
	private static final String ELECTED_SERVER_LEADER_DYNAMIC_NODE_PATH = "/dynamic";

	private final int id;
	private final ZooKeeperService zooKeeperService;

	private String processNodePath;
	private String watchedNodePath;
	private String watchedDynamicNodePath;

	Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();

	public ProcessNode(final int id, final String zkURL) throws IOException {
		this.id = id;
		zooKeeperService = new ZooKeeperService(zkURL, new ProcessNodeWatcher());
	}

	private void attemptForLeaderPosition() throws KeeperException, InterruptedException {

		final List<String> childNodePaths = zooKeeperService.getChildren(LEADER_ELECTION_ROOT_NODE, false);

		Collections.sort(childNodePaths);
		LOG.error("processNodePath = " + childNodePaths);

		int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
		if (index == 0) {
			if (true) {
				InetAddress ip;
				try {
					ip = InetAddress.getLocalHost();
					String hostName = ip.getHostName();
					final String ServerLeaderNodePath = zooKeeperService.createNode(ELECTED_SERVER_LEADER_NODE_PATH,
							false, false);
					LOG.info("[Process: " + id + "with hostName:: " + hostName + "] I am the new leader!");
					String data = zooKeeperService.setNodeData(ServerLeaderNodePath, hostName);
					if (data != hostName) {
						System.out.println("Error in setting the data for zNode" + ServerLeaderNodePath);
					} else {
						System.out.println("Data for zNode " + ServerLeaderNodePath + "set successfully to " + data);
					}
					String dynamicPath = ELECTED_SERVER_LEADER_DYNAMIC_NODE_PATH + "/G4CMONITOR";
					Stat dynamicStat;
					System.out.println("going to create dynamic node path for watching :: " + dynamicPath);
					try {
						System.out.println("Inside Try for dynamic node creation");
						dynamicStat = zooKeeperService.getZooKeeper().exists(dynamicPath, false);
						System.out.println("Stat :: " + dynamicStat);
						if (dynamicStat == null) {
							List<String> nodeList = zooKeeperService.parseZNodePath(dynamicPath);
							nodeList.forEach(nodeItem -> {
								System.out.println("nodeITem :: " + nodeItem);
								zooKeeperService.checkZNodeORCreate(nodeItem);
							});
							System.out.println("Completed creating the  :: " + dynamicPath);

						}
						List<ConfigData> watchedNodesString = getStaticNodeList();
						watchedNodesString.forEach(node -> {
							this.watchedDynamicNodePath = dynamicPath + "/" + node.getZnode().getHost();
							zooKeeperService.watchNode(this.watchedDynamicNodePath, true);
							LOG.info("Watch created on Znode  [ " + watchedDynamicNodePath + " ]");

						});

					} catch (KeeperException e) {

						System.out.println(e);
						throw e;
					} catch (InterruptedException e) {

						System.out.println(e);
						throw e;

					}
				} catch (UnknownHostException e) {
					System.out.println("attemptForLeaderPosition:: unable to retrieve Hostname");
					e.printStackTrace();
				}

			}
		} else {
			final String watchedNodeShortPath = childNodePaths.get(index - 1);

			watchedNodePath = LEADER_ELECTION_ROOT_NODE + "/" + watchedNodeShortPath;

			if (LOG.isInfoEnabled()) {
				LOG.info("[Process: " + id + "] - Setting watch on node with path: " + watchedNodePath);
			}
			zooKeeperService.watchNode(watchedNodePath, true);

		}
	}

	@Override
	public void run() {

		if (LOG.isInfoEnabled()) {
			LOG.info("Process with id: " + id + " has started!");
		}

		final String rootNodePath = zooKeeperService.createNode(LEADER_ELECTION_ROOT_NODE, false, false);
		if (rootNodePath == null) {
			throw new IllegalStateException(
					"Unable to create/access leader election root node with path: " + LEADER_ELECTION_ROOT_NODE);
		}

		processNodePath = zooKeeperService.createNode(rootNodePath + PROCESS_NODE_PREFIX, false, true);
		if (processNodePath == null) {
			throw new IllegalStateException(
					"Unable to create/access process node with path: " + LEADER_ELECTION_ROOT_NODE);
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("[Process: " + id + "] Process node created with path: " + processNodePath);
		}

		try {
			attemptForLeaderPosition();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public class ProcessNodeWatcher implements Watcher {

		// private static final String DATA_PATH = "/data";

		@Override
		public void process(WatchedEvent event) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("[Process: " + id + "] Event received: " + event);
				LOG.debug("EventPath = " + event.getPath());
			}

			final EventType eventType = event.getType();
			System.out.println("Event Type:: " + eventType);
			// Changes for handling NodeChildChanged
			if (EventType.NodeChildrenChanged.equals(eventType)) {

				Watcher.Event.EventType[] childNodeEvents = eventType.values();
				System.out.println("nodeChildren changed evnt :: " + childNodeEvents);
				for (Watcher.Event.EventType evt : childNodeEvents) {
					System.out.println("Recived event :: " + evt + "for nodePath");
					if (EventType.NodeDeleted.equals(eventType)) {

					} else if (EventType.NodeDeleted.equals(eventType)) {

					}
				}

			}
			if (EventType.NodeDeleted.equals(eventType)) {
				// Leader died
				if (event.getPath().equalsIgnoreCase(watchedNodePath)) {

					try {
						attemptForLeaderPosition();
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					// Client died
					String deadClient = event.getPath();
					System.out.println("Dead Client is :: " + deadClient);
					try {
						ConfigData deadConfigData = getClientData(new ConfigData.zNodeInfo(deadClient));
						List<ConfigData> runningConfigs = getRunningNodeList();

						deadConfigData.getQueueIds().forEach(queueId -> {
							for (ConfigData node : runningConfigs) {
								System.out.println("QueueID:: " + queueId);
								if (node.getQueueIds().size() + 1 < node.getMaxQueueSize()) {
									assignQueueId(node, queueId);
								}

							}

						});
					} catch (KeeperException | InterruptedException e) {

						// System.out.println("Exeption in handling deadClientData when Client Died");
						throw new IllegalStateException("Exeption in handling deadClientData when Client Died" + e);

					}

				}
			}
			if (EventType.NodeCreated.equals(eventType)) {
				String newClient = event.getPath();
				try {

					ConfigData newConfigData = getStaticClientData(new ConfigData.zNodeInfo(newClient));// getClientData
					System.out.println("newConfigData :: " + newConfigData);
					String staticPath = newConfigData.getZnode().getStaticPath();
					List<ConfigData> runningConfigs = getRunningNodeList();
					List<ConfigData> staticConfig = getStaticNodeList();
					Stat newStat = zooKeeperService.getZooKeeper().exists(newConfigData.getZnode().getDynamicPath(),
							true);
					// "/static/client/app/qid"
					// "/dynamic/client/app/qid"
					staticConfig.forEach(znodePath -> {
						System.out.println("FOREACH :: " + znodePath);

						/*
						 * int index = znodePath.toString().indexOf("/", 1); String strZnode =
						 * znodePath.toString().substring(index)
						 * 
						 * index = newConfigData.getZnodePath().toString().indexOf("/", 1); String
						 * strNewZnodePath = newConfigData.getZnodePath().toString().substring(index);
						 */

						System.out.println("znodePath:: " + znodePath);
						System.out.println("staticPath:: " + staticPath);
						if (znodePath.getZnode().getStaticPath().equals(staticPath)) {// fix client name
							// node matches then find the queue id's supposed to be assigned to this node
							System.out.println("Mathced znodePath and staticPath" + staticPath);
							znodePath.getQueueIds().forEach(queueId -> {
								boolean found = false;
								for (ConfigData node : runningConfigs) {
									System.out.println("Running config nocde:: " + node);

									if (node.getQueueIds().contains(queueId)) {
										deleteQId(node, queueId);
										found = true;
										assignQueueId(node, queueId);
									}

								}
								if (!found)
									assignQueueId(newConfigData);
							});
						}

					});
				} catch (KeeperException | InterruptedException e) {
					throw new IllegalStateException("Exception ProcessNodeWatcher:: in handling NodeCreated Event" + e);
				}
			}
		} // End

		private void assignQueueId(ConfigData node) {

			System.out.println("QueueID is already  assigned for :: " + node);
			try {
				Stat stat = zooKeeperService.getZooKeeper().exists(node.getZnode().getDataPath(), false);
				if (stat == null) {
					List<String> nodeList = zooKeeperService.parseZNodePath(node.getZnode().getDataPath());
					nodeList.forEach(nodeItem -> {

						zooKeeperService.checkZNodeORCreate(nodeItem);
					});

				}
				stat = zooKeeperService.getZooKeeper().exists(node.getZnode().getDataPath(), false);
				if (stat != null) {
					zooKeeperService.getZooKeeper().setData(node.getZnode().getDataPath(), gson.toJson(node).getBytes(),
							stat.getVersion());
				} else {
					// Stat stat =
					// zooKeeperService.getZooKeeper().exists(node.getZnode().getDataPath(), false);
					zooKeeperService.getZooKeeper().setData(node.getZnode().getDataPath(), gson.toJson(node).getBytes(),
							0);
				}

			} catch (KeeperException | InterruptedException e) {
				throw new IllegalStateException("Exception in assignQueueId::  " + e);
			}

		}

		private ConfigData getStaticClientData(zNodeInfo zNodeInfo) throws KeeperException, InterruptedException {

			return readDataFromNode(zNodeInfo.getStaticPath());
		}

		private void deleteQId(ConfigData node, String queueId) {
			node.getQueueIds().remove(queueId);
			System.out.println("QueueID is deleted for :: " + node + "  removed :: " + queueId);
			try {
				zooKeeperService.getZooKeeper().setData(node.getZnode().getDataPath(), gson.toJson(node).getBytes(),
						node.getStat().getVersion());
			} catch (KeeperException | InterruptedException e) {
				throw new IllegalStateException("Exception in deleteQId::  " + e);
			}

		}

		private void assignQueueId(ConfigData node, String queueId) {
			node.getQueueIds().add(queueId);
			System.out.println("QueueID is assigned for :: " + node + "  assigned :: " + queueId);
			try {
				Stat stat = zooKeeperService.getZooKeeper().exists(node.getZnode().getDataPath(), false);
				if (stat == null) {
					List<String> nodeList = zooKeeperService.parseZNodePath(node.getZnode().getDataPath());
					nodeList.forEach(nodeItem -> {

						zooKeeperService.checkZNodeORCreate(nodeItem);
					});
				}
				if (node.getStat() != null) {
					zooKeeperService.getZooKeeper().setData(node.getZnode().getDataPath(), gson.toJson(node).getBytes(),
							node.getStat().getVersion());
				} else {
					zooKeeperService.getZooKeeper().setData(node.getZnode().getDataPath(), gson.toJson(node).getBytes(),
							0);
				}

			} catch (KeeperException | InterruptedException e) {
				throw new IllegalStateException("Exception in assignQueueId::  " + e);
			}

		}

		private List<ConfigData> getRunningNodeList() {

			List<String> runningNodes = zooKeeperService.getChildren("/dynamic/G4CMONITOR", false);
			System.out.println("Running Nodes :: " + runningNodes);
			List<ConfigData> runningConfig = new ArrayList<>();
			runningNodes.forEach(zpath -> {
				try {
					Stat stat = null;
					System.out.println("runningNodes.Node:: " + zpath);
					ConfigData.zNodeInfo nodeTemp = new ConfigData.zNodeInfo("dynamic", "G4CMONITOR", zpath);
					System.out.println("nodTemp :: " + nodeTemp);
					
					if (checkDataNodeExist(nodeTemp)) {
						runningConfig.add(getStaticClientData(nodeTemp));
					} else {

						byte[] data = zooKeeperService.getZooKeeper().getData(nodeTemp.getDataPath(), false, stat);
						if (data != null) {

							String strData = new String(data);
							if (!strData.isEmpty()) {
								System.out.println("Node:: " + strData);
								ConfigData zConfigData = gson.fromJson(strData, ConfigData.class);
								System.out.println("zconfigNodeData:: " + zConfigData);

								zConfigData.setZnodePath(nodeTemp.getDynamicPath());
								zConfigData.setStat(stat);
								runningConfig.add(zConfigData);
							}
						}
					}
				} catch (KeeperException | InterruptedException e) {
					throw new IllegalStateException("Exception in getRunningNodeList::  " + e);
				}
			});
			System.out.println("RunningConfig :: " + runningConfig);
			return runningConfig;
		}

		private boolean checkDataNodeExist(zNodeInfo nodeTemp) {
			System.out.println("in checkDataNodeExist [" + nodeTemp.getDataPath() + "]");
			Stat stat;
			try {
				stat = zooKeeperService.getZooKeeper().exists(nodeTemp.getDataPath(), false);
				if (stat == null) {
					System.out.println("Stat is null: retrun  false");
					return false;
				}

				return true;
			} catch (KeeperException | InterruptedException e) {
				System.out.println(e);
				return false;
			}

		}

		private ConfigData getClientData(zNodeInfo zNodeInfo) throws KeeperException, InterruptedException {

			return readDataFromNode(zNodeInfo.getDataPath());
		}

		public ConfigData readDataFromNode(String dataClientPath) throws KeeperException, InterruptedException {
			byte[] data;
			ConfigData nodeConfigData;

			try {
				data = zooKeeperService.getZooKeeper().getData(dataClientPath, false, null);
				
				if (data == null ) {
					nodeConfigData = new ConfigData();

				} else {
					String strData = new String(data);
					if (strData.isEmpty()) {
						nodeConfigData = new ConfigData();
					} else {
						System.out.println("Data in readDataFromNode::  recieved ::" + new String(data) + " for node:: "
								+ dataClientPath);

						nodeConfigData = gson.fromJson(strData, ConfigData.class);
					}

				}
				nodeConfigData.setZnodePath(dataClientPath);
				return nodeConfigData;

			} catch (KeeperException | InterruptedException e) {
				throw new IllegalStateException("Exception in readDataFromNode::  " + e);
			}

		}

	}

	public List<ConfigData> getStaticNodeList() {

		List<String> staticNodes = zooKeeperService.getChildren("/static/G4CMONITOR", true);
		System.out.println("ZnodePathChildren :: " + staticNodes);
		List<ConfigData> staticConfig = new ArrayList<>();
		staticNodes.forEach(zpath -> {

			try {
				System.out.println("Node:: " + zpath);
				ConfigData.zNodeInfo nodeTemp = new ConfigData.zNodeInfo("static", "G4CMONITOR", zpath);
				System.out.println("nodTemp :: " + nodeTemp);
				byte[] data = zooKeeperService.getZooKeeper().getData(nodeTemp.getStaticPath(), false, null);
				if (data != null) {
					String strData = new String(data);
					if (!strData.isEmpty()) {
						System.out.println("Node:: " + strData);
						ConfigData zConfigData = gson.fromJson(strData, ConfigData.class);
						System.out.println("zconfigNodeData:: " + zConfigData);
						zConfigData.setZnodePath(nodeTemp.getStaticPath());
						// zConfigData.setStat(stat);
						staticConfig.add(zConfigData);
					}

				}
			} catch (Exception e) {
				throw new IllegalStateException("Exception in getStaticNodeList::  " + e);
			}
		});
		return staticConfig;
	}

}
