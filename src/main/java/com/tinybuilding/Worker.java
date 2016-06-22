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
import java.util.Random;

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
