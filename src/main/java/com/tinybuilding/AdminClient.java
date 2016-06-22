package com.tinybuilding;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;

public class AdminClient implements Watcher {
    ZooKeeper zk;

    AdminClient() {
    }

    void start() throws Exception {
        zk = new ZooKeeper("192.168.1.228:2181", 15000, this);
    }

    void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte masterData[] = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) +
                    " since " + startDate);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("No Master");
        }
        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + ": " + state);
        }
        System.out.println("Tasks:");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception {
        AdminClient c = new AdminClient();
        c.start();
        c.listState();
    }
}