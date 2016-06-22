package com.tinybuilding;

import org.apache.zookeeper.*;

public class Client implements Watcher {
    ZooKeeper zk;
    String hostPort;

    Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws Exception {
        zk = new ZooKeeper("192.168.1.228:2181", 15000, this);
    }

    String queueCommand(String command) throws KeeperException {
        String name = "";

        while (true) {
            try {
                name = zk.create("/tasks/task-",
                        command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                return name;
//            break;
            } catch (KeeperException.NodeExistsException e) {
                throw new KeeperException.NodeExistsException(name + " already appears to be running");
            } catch (KeeperException.ConnectionLossException e) {

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public static void main(String args[]) throws Exception {
        Client c = new Client("2181");
        c.startZK();
        String name = c.queueCommand("processRequest");
        System.out.println("Created " + name);
    }
}