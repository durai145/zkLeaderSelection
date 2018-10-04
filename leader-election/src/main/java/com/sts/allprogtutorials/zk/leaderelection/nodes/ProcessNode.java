/**
 * 
 */
package com.sts.allprogtutorials.zk.leaderelection.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sts.allprogtutorials.zk.leaderelection.main.ConfigData;
import com.sts.allprogtutorials.zk.utils.ZooKeeperService;
import java.net.InetAddress;

/**
 * @author Sain Technology Solutions
 *
 */
public class ProcessNode implements Runnable {

	private static final Logger LOG = Logger.getLogger(ProcessNode.class);

	private static final String LEADER_ELECTION_ROOT_NODE = "/election";
	private static final String PROCESS_NODE_PREFIX = "/p_";

	private final int id;
	private final ZooKeeperService zooKeeperService;

	private String processNodePath;
	private String watchedNodePath;

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
				LOG.info("[Process: " + id + "] I am the new leader!");
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
			if(LOG.isDebugEnabled()) {
				LOG.debug("[Process: " + id + "] Event received: " + event);
			}
			
			final EventType eventType = event.getType();
			if(EventType.NodeDeleted.equals(eventType)) {
				// Leader died
				if(event.getPath().equalsIgnoreCase(watchedNodePath)) {
					attemptForLeaderPosition();
				} else 
				{
					//Client died
					String deadClient = event.getPath();
					
					try {
						ConfigData deadConfigData = getDeadClientData(deadClient);
						List<ConfigData> runningConfigs= getRunningNodeList();
						
						deadConfigData.getQueueIds().forEach(queueId -> {
							for (ConfigData node: runningConfigs) {
								if (node.getQueueIds().size() + 1 < node.getMaxQueueSize()) {
									assignQueueId(node, queueId);
								}

							}
							
						});
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					
							
				}
			}
			if(EventType.NodeCreated.equals(eventType)) {
				String newClient = event.getPath();				
				try {
					
					ConfigData newConfigData = getDeadClientData(newClient);//getClientData
					List<ConfigData> runningConfigs= getRunningNodeList();
					List<ConfigData> staticConfig = getStaticNodeList();
					//"/staict/client/app/qid"
					//"/dynamic/client/app/qid"
					staticConfig.forEach(znodePath -> {
						if (znodePath.equals(newConfigData.getZnodePath())) {//fix client name
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
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}			
			}
		} // End

		private void deleteQId(ConfigData node, String queueId) {
			node.getQueueIds().remove(queueId);
			
			try {
				zooKeeperService.getZooKeeper().setData(node.getZnodePath(), gson.toJson(node).getBytes(), node.getStat().getVersion());
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		private void assignQueueId(ConfigData node, String queueId) {
			node.getQueueIds().add(queueId);

			try {
				zooKeeperService.getZooKeeper().setData(node.getZnodePath(), gson.toJson(node).getBytes(), node.getStat().getVersion());
			} catch (KeeperException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public List<ConfigData> getStaticNodeList() {

			List<String> staticNodes = zooKeeperService.getChildren("/static", true);
			List<ConfigData> staticConfig = new ArrayList<>();
			staticNodes.forEach(zpath -> {
				try {
					byte[] data = zooKeeperService.getZooKeeper().getData(zpath, false, null);
					String strData = new String(data);
					ConfigData zConfigData = gson.fromJson(strData, ConfigData.class);
					staticConfig.add(zConfigData);

				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});

			return staticConfig;
		}

		private List<ConfigData> getRunningNodeList() {

			List<String> runningNodes = zooKeeperService.getChildren("/dynamic", true);
			List<ConfigData> runningConfig = new ArrayList<>();
			runningNodes.forEach(zpath -> {
				try {
					Stat stat=null; //TODO:
					byte[] data = zooKeeperService.getZooKeeper().getData(zpath, false, stat);
					String strData = new String(data);
					ConfigData zConfigData = gson.fromJson(strData, ConfigData.class);
					zConfigData.setZnodePath(zpath);
					zConfigData.setStat(stat);
					runningConfig.add(zConfigData);

				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});

			return runningConfig;
		}

		private ConfigData getDeadClientData(String deadClient) throws KeeperException, InterruptedException {

			String dataClientPath = DATA_PATH + deadClient;
			return readDataFromNode(dataClientPath);
		}

		private ConfigData readDataFromNode(String dataClientPath) throws KeeperException, InterruptedException {
			byte[] data;

			try {
				data = zooKeeperService.getZooKeeper().getData(dataClientPath, false, null);
				String strData = new String(data);
				ConfigData nodeConfigData = gson.fromJson(strData, ConfigData.class);
				return nodeConfigData;
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

	}

}
