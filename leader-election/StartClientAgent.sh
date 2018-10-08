echo "Starting Server Agent "
hostname
java -cp target/leader-election-0.0.1-SNAPSHOT.jar com.sts.allprogtutorials.zk.leaderelection.main.ClientAgent 18.235.45.11:2181,18.214.208.121:2181,35.175.71.81:2181
