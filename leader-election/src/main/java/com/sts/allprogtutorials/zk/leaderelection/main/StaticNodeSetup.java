package com.sts.allprogtutorials.zk.leaderelection.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sts.allprogtutorials.zk.leaderelection.nodes.ProcessNode.ProcessNodeWatcher;
import com.sts.allprogtutorials.zk.utils.ZooKeeperService;

public class StaticNodeSetup {
//"18.210.40.41:2181,35.175.71.81:2181,35.175.71.81:2181"
	private static final String PATH_SEPRATOR = "/";
	private static ZooKeeper zooKeeper;
	private static ZooKeeperService zooKeeperService;
	static Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
	public static void main(String[] argv) throws IOException {

		if (argv.length < 2) {
			usage();
		}
        
		String url = argv[0];
		File file = new File(argv[1]);
		String line = "";
		zooKeeperService = new ZooKeeperService(url, null);
		
		try {
			BufferedReader br;
			zooKeeper = new ZooKeeper(url, 3000, null);

			br = new BufferedReader(new FileReader(file));
			while ((line = br.readLine()) != null) {
				String[] strArray = line.split("=");
				String zNodPath = strArray[0];
				List<String> allRootPath = zooKeeperService.parseZNodePath(zNodPath);
				// checkNode is exit
				System.out.println("allRootPath = " + allRootPath);
				System.out.println("strArray[0] :: " + strArray[0]); 
				System.out.println("strArray[1] :: " + strArray[1]); 
				
				ConfigData nodeConfigData = gson.fromJson(strArray[1], ConfigData.class);				
				
				allRootPath.forEach(node -> {

					zooKeeperService.checkZNodeORCreate(node);
				});
				Stat nodeStat = zooKeeper.exists(zNodPath, false);
				
				zooKeeper.setData(zNodPath,gson.toJson(nodeConfigData).getBytes(), nodeStat.getVersion());
			}
			br.close();
		} catch (IOException e) {
			
		    e.printStackTrace();
			throw new IllegalStateException("Error while reading " + file + e);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
			throw new IllegalStateException("StaticNodeSetup:: Exception in main()" + e);
		} 

	}

	private static void usage() {
		System.out.println("StaticNodeSetup <IPaddress:port list> <filename> ");

	}
}
