package com.tinybuilding;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort;
    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    String serverId = Long.toString(new Random().nextLong());
    static boolean isLeader = false;

//    static boolean isLeader;
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") +
                    "the leader");
        }
    };
    void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data,
                           Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    return;
            }
        }
    };

    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    Watcher masterExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals( e.getPath() );
                runForMaster();
            }
        }
    };

//    // returns true if there is a master
//    boolean checkMaster() {
//        while (true) {
//            try {
//                Stat stat = new Stat();
//                byte data[] = zk.getData("/master", false, stat);
//                isLeader = new String(data).equals(serverId);
//                return true;
//            } catch (KeeperException.NoNodeException e) {
//// no master, so try create again
//                return false;
//            } catch (KeeperException.ConnectionLossException e) {
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            } catch (KeeperException e) {
//                e.printStackTrace();
//            }
//        }
//    }
//    void runForMaster() throws InterruptedException {
//        while (true) {
//            try {
//                zk.create("/master", serverId.getBytes(),
//                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//                isLeader = true;
//                break;
//            } catch (KeeperException.NodeExistsException e) {
//                isLeader = false;
//                break;
//            } catch (KeeperException.ConnectionLossException e) {
//            } catch (KeeperException e) {
//                e.printStackTrace();
//            }
//            if (checkMaster()) break;
//        }
//    }

    void startZK() {
        try {
            zk = new ZooKeeper(Config.CONNECT_STR, 15000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public static void main(String args[])
            throws Exception {
        String port = "2181";
        Master m = new Master(port);
        m.startZK();

        m.runForMaster();
        if (isLeader) {
            System.out.println("I'm the leader");
// wait for a bit
            Thread.sleep(60000);
        } else {
            System.out.println("Someone else is the leader");
        }
//        m.stopZK();
// wait for a bit
        Thread.sleep(60000);
    }
}
