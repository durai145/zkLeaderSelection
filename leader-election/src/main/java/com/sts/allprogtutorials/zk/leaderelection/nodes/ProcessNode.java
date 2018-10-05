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

	private void attemptForLeaderPosition() {

		final List<String> childNodePaths = zooKeeperService.getChildren(LEADER_ELECTION_ROOT_NODE, false);

		Collections.sort(childNodePaths);
		LOG.error("processNodePath = " + childNodePaths);

		int index = childNodePaths.indexOf(processNodePath.substring(processNodePath.lastIndexOf('/') + 1));
		if (index == 0) {
			if (LOG.isInfoEnabled()) {
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
						System.out.println("Data for zNode" + ServerLeaderNodePath + "set successfully to " + data);
					}
					final String dynamicPath = zooKeeperService.createNode(ELECTED_SERVER_LEADER_DYNAMIC_NODE_PATH,
							false, false);
					if (dynamicPath == ELECTED_SERVER_LEADER_DYNAMIC_NODE_PATH) {
						this.watchedDynamicNodePath = dynamicPath;
						zooKeeperService.watchNode(watchedDynamicNodePath, true);
					} else {
						System.out.println("attemptForLeaderPosition:: unable to create znode "
								+ ELECTED_SERVER_LEADER_DYNAMIC_NODE_PATH);
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

		attemptForLeaderPosition();
	}

	public class ProcessNodeWatcher implements Watcher {

		private static final String DATA_PATH = "/data/";

		@Override
		public void process(WatchedEvent event) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("[Process: " + id + "] Event received: " + event);
				LOG.debug("EventPath = " + event.getPath());
			}

			final EventType eventType = event.getType();
			if (EventType.NodeDeleted.equals(eventType)) {
				// Leader died
				if (event.getPath().equalsIgnoreCase(watchedNodePath)) {

					attemptForLeaderPosition();
				} else {
					// Client died
					String deadClient = event.getPath();

					try {
						ConfigData deadConfigData = getClientData(deadClient);
						List<ConfigData> runningConfigs = getRunningNodeList();

						deadConfigData.getQueueIds().forEach(queueId -> {
							for (ConfigData node : runningConfigs) {
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

					ConfigData newConfigData = getClientData(newClient);// getClientData
					List<ConfigData> runningConfigs = getRunningNodeList();
					List<ConfigData> staticConfig = getStaticNodeList();
					// "/static/client/app/qid"
					// "/dynamic/client/app/qid"
					staticConfig.forEach(znodePath -> {

						int index = znodePath.toString().indexOf("/", 1);
						String strZnode = znodePath.toString().substring(index);

						index = newConfigData.getZnodePath().toString().indexOf("/", 1);
						String strNewZnodePath = newConfigData.getZnodePath().toString().substring(index);

						System.out.println("strNewZnodePath:: " + strNewZnodePath);
						System.out.println("strZnode:: " + strZnode);
						if (strZnode.equals(strNewZnodePath)) {// fix client name
							// node matches then find the queue id's supposed to be assigned to this node
							for (ConfigData node : runningConfigs) {

								node.getQueueIds().forEach(queueId -> {
									if (node.getQueueIds().contains(queueId)) {
										deleteQId(node, queueId);
										assignQueueId(newConfigData, queueId);
									}

								});
							}
						}
					});
				} catch (KeeperException | InterruptedException e) {
					throw new IllegalStateException("Exception ProcessNodeWatcher:: in handling NodeCreated Event" + e);
				}
			}
		} // End

		private void deleteQId(ConfigData node, String queueId) {
			node.getQueueIds().remove(queueId);

			try {
				zooKeeperService.getZooKeeper().setData(node.getZnode().getDataPath(), gson.toJson(node).getBytes(),
						node.getStat().getVersion());
			} catch (KeeperException | InterruptedException e) {
				throw new IllegalStateException("Exception in deleteQId::  " + e);
			}

		}

		private void assignQueueId(ConfigData node, String queueId) {
			node.getQueueIds().add(queueId);

			try {
				Stat stat = zooKeeperService.getZooKeeper().exists(node.getZnode().getDataPath(), false);
				if (stat == null) {
					List<String> nodeList = zooKeeperService.parseZNodePath(node.getZnode().getDataPath());
					nodeList.forEach(nodeItem -> {

						zooKeeperService.checkZNodeORCreate(nodeItem);
					});
				}

				zooKeeperService.getZooKeeper().setData(node.getZnode().getDataPath(), gson.toJson(node).getBytes(),
						node.getStat().getVersion());

			} catch (KeeperException | InterruptedException e) {
				throw new IllegalStateException("Exception in assignQueueId::  " + e);
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
					String strData = new String(data);
					System.out.println("Node:: " + strData);
					ConfigData zConfigData = gson.fromJson(strData, ConfigData.class);
					System.out.println("zconfigNodeData:: " + zConfigData);
					staticConfig.add(zConfigData);

				} catch (Exception e) {
					throw new IllegalStateException("Exception in getStaticNodeList::  " + e);
				}
			});
			return staticConfig;
		}

		private List<ConfigData> getRunningNodeList() {

			List<String> runningNodes = zooKeeperService.getChildren("/dynamic/G4CMONITOR", true);
			List<ConfigData> runningConfig = new ArrayList<>();
			runningNodes.forEach(zpath -> {
				try {
					Stat stat = null;
					System.out.println("Node:: " + zpath);
					ConfigData.zNodeInfo nodeTemp = new ConfigData.zNodeInfo("dynamic", "G4CMONITOR", zpath);
					System.out.println("nodTemp :: " + nodeTemp);
					byte[] data = zooKeeperService.getZooKeeper().getData(nodeTemp.getDynamicPath(), false, stat);
					String strData = new String(data);
					System.out.println("Node:: " + strData);
					ConfigData zConfigData = gson.fromJson(strData, ConfigData.class);
					System.out.println("zconfigNodeData:: " + zConfigData);

					zConfigData.setZnodePath(zpath);
					zConfigData.setStat(stat);
					runningConfig.add(zConfigData);

				} catch (KeeperException | InterruptedException e) {
					throw new IllegalStateException("Exception in getRunningNodeList::  " + e);
				}
			});
			System.out.println("RunningConfig :: " + runningConfig);
			return runningConfig;
		}

		private ConfigData getClientData(String deadClient) throws KeeperException, InterruptedException {

			String dataClientPath = DATA_PATH + deadClient;
			return readDataFromNode(dataClientPath);
		}

		public ConfigData readDataFromNode(String dataClientPath) throws KeeperException, InterruptedException {
			byte[] data;

			try {
				data = zooKeeperService.getZooKeeper().getData(dataClientPath, false, null);
				String strData = new String(data);
				ConfigData nodeConfigData = gson.fromJson(strData, ConfigData.class);
				return nodeConfigData;
			} catch (KeeperException | InterruptedException e) {
				throw new IllegalStateException("Exception in readDataFromNode::  " + e);
			}

		}

	}

}
