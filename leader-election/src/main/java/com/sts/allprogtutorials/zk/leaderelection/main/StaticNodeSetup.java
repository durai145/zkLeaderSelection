package com.sts.allprogtutorials.zk.leaderelection.main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class StaticNodeSetup {

	private static ZooKeeper zooKeeper;

	public static void main(String [] argv) {
		
		if (argv.length ==0) {
			usage();
		}
		
		File file = new File(argv[0]);
		String line = "";

		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(file));
			while ((line = br.readLine()) != null) {
				String[] strArray = line.split("=");
				zooKeeper = new ZooKeeper("18.210.40.41:2181,35.175.71.81:2181,35.175.71.81:2181", 3000, null);
				zooKeeper.create(strArray[0], strArray[1].getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	private static void usage() {
		System.out.println("StaticNodeSetup <filename>");
		
	}
}
