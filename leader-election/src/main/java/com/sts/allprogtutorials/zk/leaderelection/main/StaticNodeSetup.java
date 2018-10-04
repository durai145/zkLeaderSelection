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

public class StaticNodeSetup {
//"18.210.40.41:2181,35.175.71.81:2181,35.175.71.81:2181"
	private static final String PATH_SEPRATOR = "/";
	private static ZooKeeper zooKeeper;
	static Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).create();
	public static void main(String[] argv) {

		if (argv.length < 2) {
			usage();
		}
        
		String url = argv[0];
		File file = new File(argv[1]);
		String line = "";

		
		try {
			BufferedReader br;
			zooKeeper = new ZooKeeper(url, 3000, null);

			br = new BufferedReader(new FileReader(file));
			while ((line = br.readLine()) != null) {
				String[] strArray = line.split("=");
				String zNodPath = strArray[0];
				List<String> allRootPath = parseZNodePath(zNodPath);
				// checkNode is exit
				System.out.println("allRootPath = " + allRootPath);
				 
				ConfigData nodeConfigData = gson.fromJson(strArray[1], ConfigData.class);				
				
				allRootPath.forEach(node -> {

					checkZNodeORCreate(node);
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

	private static void checkZNodeORCreate(String node) {
		try {
			System.out.println("checkZNodeORCreate::Node :: " + node);
			Stat nodeStat = zooKeeper.exists(node, false);
			System.out.println("Nodestat :: " + nodeStat);
			if (nodeStat == null) {
				String nodePath = zooKeeper.create(node, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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

	private static List<String> parseZNodePath(String zNodPath) {
		List<String> nodes = new ArrayList<>();
		String[] dirs = zNodPath.split("/");
		String parent = "";
		for (String dir : dirs) {
			if (dir != null && !dir.isEmpty()) {
				parent = parent + PATH_SEPRATOR + dir;
				nodes.add(parent);
			}
		}
		return nodes;
	}

	private static void usage() {
		System.out.println("StaticNodeSetup <IPaddress:port list> <filename> ");

	}
}
