package com.tinybuilding;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Hello world!
 *
 */
public class Master implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    MasterStates state = null;

    ZooKeeper zk;
    String hostPort;
    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    String serverId = Long.toString(new Random().nextLong());
    static boolean isLeader = false;


    private void takeLeadership() {
        isLeader = true;
    }

//    static boolean isLeader;
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            LOG.info("master create call back: " + KeeperException.Code.get(rc).name());
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    state = MasterStates.ELECTED;
                    takeLeadership();
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    LOG.error("Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            System.out.println("I'm " + (isLeader ? "" : "not ") +
                    "the leader");
        }
    };
    void runForMaster() {
        LOG.info("run for master...");
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc, String path, Object ctx, byte[] data,
                           Stat stat) {
            LOG.info("master check call back: " + KeeperException.Code.get(rc).name());
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

    void masterExists() {
        zk.exists("/master",
                masterExistsWatcher,
                masterExistsCallback,
                null);
    }

    Watcher masterExistsWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            LOG.info("master exist watcher: " + e.getPath() + " " + e.getState() + " " + e.getType());
            if(e.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals( e.getPath() );
                runForMaster();
            }
        }
    };

    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            LOG.info("master exist call back: " + KeeperException.Code.get(rc).name());
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    masterExists();
                    break;
                case OK:
                    if(stat == null) {
                        state = MasterStates.RUNNING;
                        runForMaster();
                    }
                    break;
                default:
                    checkMaster();
                    break;
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

    List<String> workerList;

    Watcher workersChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/workers".equals( e.getPath() );
                getWorkers();
            }
        }
    };
    void getWorkers() {
        zk.getChildren("/workers",
                workersChangeWatcher,
                workersGetChildrenCallback,
                null);
    }
    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        public void processResult(int rc, String path, Object ctx,
                                  List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getWorkers();
                    break;
                case OK:
                    LOG.info("Succesfully got a list of workers: "
                            + children.size()
                            + " workers");
                    reassignAndSet(children);
                    break;
                default:
                    LOG.error("getChildren failed",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    ChildrenCache workersCache;
    void reassignAndSet(List<String> children) {
        List<String> toProcess;
        if(workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info( "Removing and setting" );
            toProcess = workersCache.removedAndSet( children );
        }
        if(toProcess != null) {
            for(String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    void getAbsentWorkerTasks(String worker) {

    }

    Watcher tasksChangeWatcher = new Watcher() {
        public void process(WatchedEvent e) {
            if(e.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/tasks".equals( e.getPath() );
                getTasks();
            }
        }
    };
    void getTasks() {
        zk.getChildren("/tasks",
                tasksChangeWatcher,
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
                        assignTasks(children);
                    }
                    break;
                default:
                    LOG.error("getChildren failed.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    //////assign tasks /////////////
    void assignTasks(List<String> tasks) {
        for(String task : tasks) {
            getTaskData(task);
        }
    }
    void getTaskData(String task) {
        zk.getData("/tasks/" + task,
                false,
                taskDataCallback,
                task);
    }
    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        public void processResult(int rc,
                                  String path,
                                  Object ctx,
                                  byte[] data,
                                  Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTaskData((String) ctx);
                    break;
                case OK:
                    /*
                    * Choose worker at random.
                    */
                    int worker = new Random().nextInt(workerList.size());
                    String designatedWorker = workerList.get(worker);
                    /*
                    * Assign task to randomly chosen worker.
                    */
                    String assignmentPath = "/assign/" + designatedWorker + "/" +
                            (String) ctx;
                    createAssignment(assignmentPath, data);
                    break;
                default:
                    LOG.error("Error when trying to get task data.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void createAssignment(String path, byte[] data) {
        zk.create(path,
                data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                assignTaskCallback,
                data);
    }
    AsyncCallback.StringCallback assignTaskCallback = new AsyncCallback.StringCallback() {
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createAssignment(path, (byte[]) ctx);
                    break;
                case OK:
                    LOG.info("Task assigned correctly: " + name);
                    deleteTask(name.substring( name.lastIndexOf("/") + 1 ));
                    break;
                case NODEEXISTS:
                    LOG.warn("Task already assigned");
                    break;
                default:
                    LOG.error("Error when trying to assign task.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void deleteTask(String task) {
        zk.delete("/tasks/" + task, 0, deleteTaskCallback, task);
    }

    AsyncCallback.VoidCallback deleteTaskCallback = new AsyncCallback.VoidCallback() {
        public void processResult(int rc, String path, Object ctx) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteTask(String.valueOf(ctx));
                    break;
                case OK:
                    LOG.info("Delete Task correctly: " + ctx);
                    break;
                default:
                    LOG.error("Error when trying to delete task.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };



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
        Thread.sleep(600000000);
    }
}
