package etl.util;

import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.curator.utils.ZKPaths;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

/**
 * A <a href="package.html">protocol to implement an exclusive
 *  write lock or to elect a leader</a>. <p/> You invoke {@link #lock()} to 
 *  start the process of grabbing the lock; you may get the lock then or it may be 
 *  some time later. <p/> You can register a listener so that you are invoked 
 *  when you get the lock; otherwise you can ask if you have the lock
 *  by calling {@link #isOwner()}
 *
 */
public class DistributedLock implements Watcher {
    private static final Logger LOG = LogManager.getLogger(DistributedLock.class);

    private final String dir;
    private String id;
    private ZNodeName idName;
    private String ownerId;
    private String lastChildId;
    private byte[] data = {0x12, 0x34};
    private LockListener callback;
    
    /**
     * This class has two methods which are call
     * back methods when a lock is acquired and 
     * when the lock is released.
     *
     */
    public interface LockListener {
        /**
         * call back called when the lock 
         * is acquired
         */
        public void lockAcquired();
        
        /**
         * call back called when the lock is 
         * released.
         */
        public void lockReleased();
    }
    
    /**
     * zookeeper contructor for writelock
     * @param zookeeperConnStr the zookeeper connection string
     * @param dir the parent path you want to use for locking
     * @param acls the acls that you want to use for all the paths, 
     * if null world read/write is used.
     * @throws IOException 
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    public DistributedLock(String zookeeperConnStr, String dir, List<ACL> acl) throws IOException, KeeperException, InterruptedException {
        this.zookeeper = new ZooKeeper(zookeeperConnStr, 3000, this);;
        this.dir = dir;
        if (acl != null) {
            setAcl(acl);
        }
        ZKPaths.mkdirs(zookeeper, dir);
    }
    
    /**
     * zookeeper contructor for writelock with callback
     * @param zookeeper the zookeeper connection string
     * @param dir the parent path you want to use for locking
     * @param acl the acls that you want to use for all the paths
     * @param callback the call back instance
     * @throws IOException 
     * @throws InterruptedException 
     * @throws KeeperException 
     */
    public DistributedLock(String zookeeperConnStr, String dir, List<ACL> acl, 
            LockListener callback) throws IOException, KeeperException, InterruptedException {
        this(zookeeperConnStr, dir, acl);
        this.callback = callback;
    }

    /**
     * return the current locklistener
     * @return the locklistener
     */
    public LockListener getLockListener() {
        return this.callback;
    }
    
    /**
     * register a different call back listener
     * @param callback the call back instance
     */
    public void setLockListener(LockListener callback) {
        this.callback = callback;
    }

    /**
     * Removes the lock or associated znode if 
     * you no longer require the lock. this also 
     * removes your request in the queue for locking
     * in case you do not already hold the lock.
     * @throws RuntimeException throws a runtime exception
     * if it cannot connect to zookeeper.
     */
    public synchronized void unlock() throws RuntimeException {
        
        if (!isClosed() && id != null) {
            // we don't need to retry this operation in the case of failure
            // as ZK will remove ephemeral files and we don't wanna hang
            // this process when closing if we cannot reconnect to ZK
            try {
                zookeeper.delete(id, -1);
            } catch (InterruptedException e) {
                LOG.warn("Caught: " + e, e);
                //set that we have been interrupted.
               Thread.currentThread().interrupt();
            } catch (KeeperException.NoNodeException e) {
                // do nothing
            } catch (KeeperException e) {
                LOG.warn("Caught: " + e, e);
                throw (RuntimeException) new RuntimeException(e.getMessage()).
                    initCause(e);
            }
            finally {
                if (callback != null) {
                    callback.lockReleased();
                }
                id = null;
            }
        }
    }
    
    /**
     * Represents an ephemeral znode name which has an ordered sequence number
     * and can be sorted in order
     *
     */
    private class ZNodeName implements Comparable<ZNodeName> {
        private final String name;
        private String prefix;
        private int sequence = -1;
        
        public ZNodeName(String name) {
            if (name == null) {
                throw new NullPointerException("id cannot be null");
            }
            this.name = name;
            this.prefix = name;
            int idx = name.lastIndexOf('-');
            if (idx >= 0) {
                this.prefix = name.substring(0, idx);
                try {
                    this.sequence = Integer.parseInt(name.substring(idx + 1));
                    // If an exception occurred we misdetected a sequence suffix,
                    // so return -1.
                } catch (NumberFormatException e) {
                    LOG.info("Number format exception for " + idx, e);
                } catch (ArrayIndexOutOfBoundsException e) {
                   LOG.info("Array out of bounds for " + idx, e);
                }
            }
        }

        @Override
        public String toString() {
            return name.toString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ZNodeName sequence = (ZNodeName) o;

            if (!name.equals(sequence.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            return name.hashCode() + 37;
        }

        public int compareTo(ZNodeName that) {
            int answer = this.prefix.compareTo(that.prefix);
            if (answer == 0) {
                int s1 = this.sequence;
                int s2 = that.sequence;
                if (s1 == -1 && s2 == -1) {
                    return this.name.compareTo(that.name);
                }
                answer = s1 == -1 ? 1 : s2 == -1 ? -1 : s1 - s2;
            }
            return answer;
        }

        /**
         * Returns the name of the znode
         */
        public String getName() {
            return name;
        }
    }
    
    /** 
     * the watcher called on  
     * getting watch while watching 
     * my predecessor
     */
    private class LockWatcher implements Watcher {
        public void process(WatchedEvent event) {
            // lets either become the leader or watch the new/updated node
            LOG.debug("Watcher fired on path: " + event.getPath() + " state: " + 
                    event.getState() + " type " + event.getType());
            try {
                lock();
            } catch (Exception e) {
                LOG.warn("Failed to acquire lock: " + e, e);
            }
        }
    }
    
    /**
     * a zoookeeper operation that is mainly responsible
     * for all the magic required for locking.
     */
        
    /** find if we have been created earler if not create our node
     * 
     * @param prefix the prefix node
     * @param zookeeper teh zookeeper client
     * @param dir the dir paretn
     * @throws KeeperException
     * @throws InterruptedException
     */
    private void findPrefixInChildren(String prefix, ZooKeeper zookeeper, String dir) 
        throws KeeperException, InterruptedException {
        List<String> names = zookeeper.getChildren(dir, false);
        for (String name : names) {
            if (name.startsWith(prefix)) {
                id = name;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Found id created last time: " + id);
                }
                break;
            }
        }
        if (id == null) {
            id = zookeeper.create(dir + "/" + prefix, data, 
                    getAcl(), CreateMode.EPHEMERAL_SEQUENTIAL);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Created id: " + id);
            }
        }

    }
    
    /**
     * the command that is run and retried for actually 
     * obtaining the lock
     * @return if the command was successful or not
     */
    public boolean LockZooKeeperOperationExecute() throws KeeperException, InterruptedException {
        do {
            if (id == null) {
                long sessionId = zookeeper.getSessionId();
                String prefix = "x-" + sessionId + "-";
                // lets try look up the current ID if we failed 
                // in the middle of creating the znode
                findPrefixInChildren(prefix, zookeeper, dir);
                idName = new ZNodeName(id);
            }
            if (id != null) {
                List<String> names = zookeeper.getChildren(dir, false);
                if (names.isEmpty()) {
                    LOG.warn("No children in: " + dir + " when we've just " +
                    "created one! Lets recreate it...");
                    // lets force the recreation of the id
                    id = null;
                } else {
                    // lets sort them explicitly (though they do seem to come back in order ususally :)
                    SortedSet<ZNodeName> sortedNames = new TreeSet<ZNodeName>();
                    for (String name : names) {
                        sortedNames.add(new ZNodeName(dir + "/" + name));
                    }
                    ownerId = sortedNames.first().getName();
                    SortedSet<ZNodeName> lessThanMe = sortedNames.headSet(idName);
                    if (!lessThanMe.isEmpty()) {
                        ZNodeName lastChildName = lessThanMe.last();
                        lastChildId = lastChildName.getName();
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("watching less than me node: " + lastChildId);
                        }
                        Stat stat = zookeeper.exists(lastChildId, new LockWatcher());
                        if (stat != null) {
                            return Boolean.FALSE;
                        } else {
                            LOG.warn("Could not find the" +
                            		" stats for less than me: " + lastChildName.getName());
                        }
                    } else {
                        if (isOwner()) {
                            if (callback != null) {
                                callback.lockAcquired();
                            }
                            return Boolean.TRUE;
                        }
                    }
                }
            }
        }
        while (id == null);
        return Boolean.FALSE;
    }

    /**
     * Perform the given operation, retrying if the connection fails
     * @return object. it needs to be cast to the callee's expected 
     * return type.
     */
    protected Object retryOperationZop() 
        throws KeeperException, InterruptedException {
        KeeperException exception = null;
        for (int i = 0; i < retryCount; i++) {
            try {
                return LockZooKeeperOperationExecute();
            } catch (KeeperException.SessionExpiredException e) {
                LOG.warn("Session expired for: " + zookeeper + " so reconnecting due to: " + e, e);
                throw e;
            } catch (KeeperException.ConnectionLossException e) {
                if (exception == null) {
                    exception = e;
                }
                LOG.debug("Attempt " + i + " failed with connection loss so " +
                		"attempting to reconnect: " + e, e);
                retryDelay(i);
            }
        }
        throw exception;
    }
        
    /**
     * Attempts to acquire the exclusive write lock returning whether or not it was
     * acquired. Note that the exclusive lock may be acquired some time later after
     * this method has been invoked due to the current lock owner going away.
     */
    public synchronized boolean lock() throws KeeperException, InterruptedException {
        if (isClosed()) {
            return false;
        }
        ensurePathExists(dir);

        return (Boolean) retryOperationZop();
    }

    /**
     * return the parent dir for lock
     * @return the parent dir used for locks.
     */
    public String getDir() {
        return dir;
    }

    /**
     * Returns true if this node is the owner of the
     *  lock (or the leader)
     */
    public boolean isOwner() {
        return id != null && ownerId != null && id.equals(ownerId);
    }

    /**
     * return the id for this lock
     * @return the id for this lock
     */
    public String getId() {
       return this.id;
    }
    
    
    protected final ZooKeeper zookeeper;
    private AtomicBoolean closed = new AtomicBoolean(false);
    private long retryDelay = 500L;
    private int retryCount = 10;
    private List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    /**
     * Closes this strategy and releases any ZooKeeper resources; but keeps the
     *  ZooKeeper instance open
     */
    public void close() {
        if (closed.compareAndSet(false, true)) {
            doClose();
        }
    }
    
    /**
     * return zookeeper client instance
     * @return zookeeper client instance
     */
    public ZooKeeper getZookeeper() {
        return zookeeper;
    }

    /**
     * return the acl its using
     * @return the acl.
     */
    public List<ACL> getAcl() {
        return acl;
    }

    /**
     * set the acl 
     * @param acl the acl to set to
     */
    public void setAcl(List<ACL> acl) {
        this.acl = acl;
    }

    /**
     * get the retry delay in milliseconds
     * @return the retry delay
     */
    public long getRetryDelay() {
        return retryDelay;
    }

    /**
     * Sets the time waited between retry delays
     * @param retryDelay the retry delay
     */
    public void setRetryDelay(long retryDelay) {
        this.retryDelay = retryDelay;
    }

    /**
     * Allow derived classes to perform 
     * some custom closing operations to release resources
     */
    protected void doClose() {
    }

    /**
     * Ensures that the given path exists with no data, the current
     * ACL and no flags
     * @param path
     */
    protected void ensurePathExists(String path) {
        ensureExists(path, null, acl, CreateMode.PERSISTENT);
    }
    
	protected Object retryOperationEnsureExists(final String path, final CreateMode flags)
			throws KeeperException, InterruptedException {
		KeeperException exception = null;
		for (int i = 0; i < retryCount; i++) {
			try {
				Stat stat = zookeeper.exists(path, false);
				if (stat != null) {
					return true;
				}
				zookeeper.create(path, data, acl, flags);

				return true;
			} catch (KeeperException.SessionExpiredException e) {
				LOG.warn("Session expired for: " + zookeeper + " so reconnecting due to: " + e, e);
				throw e;
			} catch (KeeperException.ConnectionLossException e) {
				if (exception == null) {
					exception = e;
				}
				LOG.debug("Attempt " + i + " failed with connection loss so " + "attempting to reconnect: " + e, e);
				retryDelay(i);
			}
		}
		throw exception;
	}

    /**
     * Ensures that the given path exists with the given data, ACL and flags
     * @param path
     * @param acl
     * @param flags
     */
    protected void ensureExists(final String path, final byte[] data,
            final List<ACL> acl, final CreateMode flags) {
        try {
        	retryOperationEnsureExists(path, flags);
        } catch (KeeperException e) {
            LOG.warn("Caught: " + e, e);
        } catch (InterruptedException e) {
            LOG.warn("Caught: " + e, e);
        }
    }

    /**
     * Returns true if this protocol has been closed
     * @return true if this protocol is closed
     */
    protected boolean isClosed() {
        return closed.get();
    }

    /**
     * Performs a retry delay if this is not the first attempt
     * @param attemptCount the number of the attempts performed so far
     */
    protected void retryDelay(int attemptCount) {
        if (attemptCount > 0) {
            try {
                Thread.sleep(attemptCount * retryDelay);
            } catch (InterruptedException e) {
                LOG.debug("Failed to sleep: " + e, e);
            }
        }
    }

	@Override
	public void process(WatchedEvent event) {
		LOG.debug("Zookeeper watch event: {}", event);
	}
}
