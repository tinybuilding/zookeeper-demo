/**
 * Beijing ShineWonder Technology (ShineWonder Technology) Confidential
 * http://www.shinewonder.com
 * <p/>
 * (C) 2009 Copyright ShineWonder Tech. All right reserved.
 * <p/>
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has
 * been deposited with the China (And USA) Copyright Office.
 */
package com.tinybuilding;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by jyl on 2016/6/22.
 */
public class Worker implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    ZooKeeper zk;
    String hostPort;
    String serverId = Integer.toHexString(new Random().nextInt());

    String status = "idle";
    String name = "worker-" + serverId;

    ExecutorService executor = Executors.newCachedThreadPool();
    List<String> onGoingTasks;

    Worker(String hostPort) {
        this.hostPort = hostPort;
    }
    void startZK() throws IOException {
        zk = new ZooKeeper("192.168.1.228:2181", 15000, this);
    }
    public void process(WatchedEvent e) {
        LOG.info(e.toString() + ", " + hostPort);
    }
    void register() {
        zk.create("/workers/" + name,
                status.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null);
    }
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx,
                                  String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    LOG.info("Registered successfully: " + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered: " + serverId);
                    break;
                default:
                    LOG.error("Something went wrong: "
                            + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    ///////// task watcher   ///////////////
    Watcher newTaskWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeChildrenChanged) {
                assert new String("/assign/worker-"+ serverId).equals( e.getPath() );
                getTasks();
            }
        }
    };
    void getTasks() {
        zk.getChildren("/assign/" + name,
                newTaskWatcher,
                tasksGetChildrenCallback,
                null);
    }
    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc,
                                  String path,
                                  Object ctx,
                                  List<String> children) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if(children != null) {
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;
                            public Runnable init (List<String> children,
                                                  DataCallback cb) {
                                this.children = children;
                                this.cb = cb;
                                return this;
                            }
                            public void run() {
                                LOG.info("Looping into tasks");
                                synchronized(onGoingTasks) {
                                    for(String task : children) {
                                        if(!onGoingTasks.contains( task )) {
                                            LOG.trace("New task: {}", task);
                                            zk.getData("/assign/" + name + "/" + task,
                                                    false,
                                                    cb,
                                                    task);
                                            onGoingTasks.add( task );
                                        }
                                    }
                                }
                            }
                        }.init(children, taskDataCallback));
                    }
                    break;
                default:
                    System.out.println("getChildren failed: " +
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };





    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String)ctx);
                    return;
            }
        }
    };
    synchronized private void updateStatus(String status) {
        if (status == this.status) {
            zk.setData("/workers/" + name, status.getBytes(), -1,
                    statusUpdateCallback, status);
        }
    }
    public void setStatus(String status) {
        this.status = status;
        updateStatus(status);
    }

    public static void main(String args[]) throws Exception {
        Worker w = new Worker("2181");
        w.startZK();
        w.register();

        w.updateStatus("working");

        Thread.sleep(30000);
    }
}
