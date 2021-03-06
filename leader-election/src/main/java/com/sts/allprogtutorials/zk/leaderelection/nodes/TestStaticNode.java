package com.sts.allprogtutorials.zk.leaderelection.nodes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.KeeperException;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sts.allprogtutorials.zk.leaderelection.main.ConfigData;
import com.sts.allprogtutorials.zk.utils.ZooKeeperService;

public class TestStaticNode {
	private static ZooKeeperService zooKeeperService;
	private static Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
			.create();

	public static void main(String[] args) throws IOException {

		// TODO Auto-generated method stub
		String str = "{\"wait_time\":5,\"queue_ids\":[\"4\"],\"max_queue_size\":1,\"znode_path\":\"/static/G4CMONITOR/GPIAPP005\",\"znode\":{\"app\":\"G4CMONITOR\",\"host\":\"GPIAPP005\",\"type\":\"static\"}}";
		ConfigData config = gson.fromJson(str, ConfigData.class);
		System.out.println(config);
		System.out.println("COnfigDataa retiurned :: " + config.getZnode().getPath());
	
		//zooKeeperService = new ZooKeeperService("18.235.45.11:2181,18.214.208.121:2181,35.175.71.81:2181", null);
		//getStaticNodeList();
		
	}

	public static void getStaticNodeList() {

		List<String> staticNodes = zooKeeperService.getChildren("/static/G4CMONITOR", true);
		System.out.println("ZnodePathChildren :: " + staticNodes);
		List<ConfigData> staticConfig = new ArrayList<>();
		
		staticNodes.forEach(zpath -> {

			try {
				System.out.println("Node:: " + zpath);
				String temp = "/static/" + zpath;

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
	}
}
