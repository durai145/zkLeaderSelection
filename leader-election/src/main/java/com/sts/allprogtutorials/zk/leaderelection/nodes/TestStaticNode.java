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
	private static Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();	
	public static void main(String[] args) throws IOException {
		
		// TODO Auto-generated method stub
		zooKeeperService = new ZooKeeperService("18.235.45.11:2181,18.214.208.121:2181,35.175.71.81:2181", null);
		getStaticNodeList();
	}
	public static void getStaticNodeList() {

		List<String> staticNodes = zooKeeperService.getChildren("/static", true);
		System.out.println("ZnodePathChildren :: " + staticNodes);
		List<ConfigData> staticConfig = new ArrayList<>();
		staticNodes.forEach(zpath -> {
			try {
				System.out.println("Node:: " + zpath);
				//byte[] data = zooKeeperService.getZooKeeper().getData(zpath, false, null);
				//String strData = new String(data);
				//System.out.println("Node:: " + strData);
				//ConfigData zConfigData = gson.fromJson(strData, ConfigData.class);
				//System.out.println("zconfigNodeData:: " + zConfigData);
				//staticConfig.add(zConfigData);

			} catch (Exception e) {
				throw new IllegalStateException("Exception in getStaticNodeList::  " + e);
			}
		});
}
}
