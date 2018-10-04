/**
 * 
 */
package com.sts.allprogtutorials.zk.utils;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.sts.allprogtutorials.zk.leaderelection.nodes.ProcessNode.ProcessNodeWatcher;

/**
 * @author Sain Technology Solutions
 *
 */
public class ZooKeeperService {
	
	private ZooKeeper zooKeeper;
	
	public ZooKeeper getZooKeeper() {
		return zooKeeper;
	}

	public void setZooKeeper(ZooKeeper zooKeeper) {
		this.zooKeeper = zooKeeper;
	}

	public ZooKeeperService(final String url, final ProcessNodeWatcher processNodeWatcher) throws IOException {
		zooKeeper = new ZooKeeper(url, 3000, processNodeWatcher);
	}

	public String createNode(final String node, final boolean watch, final boolean ephimeral) {
		String createdNodePath = null;
		try {
			
			final Stat nodeStat =  zooKeeper.exists(node, watch);
			
			if(nodeStat == null) {
				createdNodePath = zooKeeper.create(node, new byte[0], Ids.OPEN_ACL_UNSAFE, (ephimeral ?  CreateMode.EPHEMERAL_SEQUENTIAL : CreateMode.PERSISTENT));
			} else {
				createdNodePath = node;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException("Exception in ZooKeeperService::createNode " + e);
		}
		
		return createdNodePath;
	}
	
	public boolean watchNode(final String node, final boolean watch) {
		
		boolean watched = false;
		try {
			final Stat nodeStat =  zooKeeper.exists(node, watch);
			
			if(nodeStat != null) {
				watched = true;
			}
			
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException("Exception in ZooKeeperService::watchNode " + e);
		}
		
		return watched;
	}
	
	public List<String> getChildren(final String node, final boolean watch) {
		
		List<String> childNodes = null;
		
		try {
			childNodes = zooKeeper.getChildren(node, watch);
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException("Exception in ZooKeeperService::getChildren " + e);
		}
		
		return childNodes;
	}

	public String setNodeData(String nodePath, String data) {
		
		try {
			final Stat stat = zooKeeper.exists(nodePath,false);
			if(stat != null)
			{
				zooKeeper.setData(nodePath, data.getBytes(), stat.getVersion());
			}
			else {
				data =  "";
			}
		} catch (KeeperException | InterruptedException e) {
			throw new IllegalStateException("Exception in ZooKeeperService::setNodeData " + e);
		}
		return data;
		
	}
	
}
