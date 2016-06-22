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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by jyl on 2016/6/22.
 */
public class MetaData implements Watcher {
    private Logger LOG = LoggerFactory.getLogger(this.getClass());

    private ZooKeeper zk;

    public MetaData() {
        try {
            zk = new ZooKeeper("192.168.1.228:2181", 15000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }
    void createParent(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data);
    }
    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn("Parent already registered: " + path);
                    break;
                default:
                    LOG.error("Something went wrong: ",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String[] args) {
        MetaData metaData = new MetaData();
        metaData.bootstrap();

        try {
            TimeUnit.SECONDS.sleep(60);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent watchedEvent) {

    }
}
