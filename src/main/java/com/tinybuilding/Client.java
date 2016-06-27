package com.tinybuilding;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class Client implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Client.class);

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


    void submitTask(String task, TaskObject taskCtx) {
        taskCtx.setTask(task);
        zk.create("/tasks/task-",
                task.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT_SEQUENTIAL,
                createTaskCallback,
                taskCtx);
    }
    AsyncCallback.StringCallback createTaskCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    submitTask(((TaskObject) ctx).getTask(),
                            (TaskObject) ctx);
                    break;
                case OK:
                    LOG.info("My created task name: " + name);
                    ((TaskObject) ctx).setTaskName(name);
                    watchStatus("/status/" + name.replace("/tasks/", ""),
                            ctx);
                    break;
                default:
                    LOG.error("Something went wrong" +
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    ConcurrentHashMap<String, Object> ctxMap =
            new ConcurrentHashMap<String, Object>();
    void watchStatus(String path, Object ctx) {
        ctxMap.put(path, ctx);
        zk.exists(path,
                statusWatcher,
                existsCallback,
                ctx);
    }
    Watcher statusWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeCreated) {
                assert e.getPath().contains("/status/task-");
                zk.getData(e.getPath(),
                false, getDataCallback, ctxMap.get(e.getPath()));
            }
        }
    };
    AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    watchStatus(path, ctx);
                    break;
                case OK:
                    if(stat != null) {
                        zk.getData(path, false, getDataCallback, null);
                    }
                    break;
                case NONODE:
                    break;
                default:
                    LOG.error("Something went wrong when " +
                            "checking if the status node exists: " +
                            KeeperException.create(KeeperException.Code.get(rc), path));
                    break;
            }
        }
    };

    AsyncCallback.DataCallback getDataCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            //TODO:
        }
    };

}