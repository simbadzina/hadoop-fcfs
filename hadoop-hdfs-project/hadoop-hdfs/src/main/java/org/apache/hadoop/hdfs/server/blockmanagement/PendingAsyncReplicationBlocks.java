package org.apache.hadoop.hdfs.server.blockmanagement;

import java.io.PrintWriter;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import java.util.Map;


import org.slf4j.Logger;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.util.Daemon;


public class PendingAsyncReplicationBlocks {
  private static final Logger LOG = BlockManager.LOG;

  private Map<Block, PendingAsyncBlockInfo> pendingReplications;
  private Map<Long, PendingAsyncBlockInfo> blockIdMapping;
  private ArrayList<Block> timedOutItems;
  Daemon timerThread = null;
  private volatile boolean fsRunning = true;

  //
  // It might take anywhere between 10 to 15 minutes before
  // a request is timed out.
  //
  private long timeout =  60  * 60 * 1000;
  private long defaultRecheckInterval = 5 * 60 * 1000;

  PendingAsyncReplicationBlocks() {
    pendingReplications = new HashMap<Block, PendingAsyncBlockInfo>();
    blockIdMapping  = new HashMap<Long,PendingAsyncBlockInfo>();

    timedOutItems = new ArrayList<Block>();
  }

  void start() {
    timerThread = new Daemon(new PendingAsyncReplicationMonitor());
    timerThread.start();
  }


  boolean isPending(Block block){
    return pendingReplications.containsKey(block);
  }
  
  int numPending(Block block){
    PendingAsyncBlockInfo found = pendingReplications.get(block);
    if(found !=null){
      return found.getNumNodes();
    }else{
      return 0;
    }
  }

  void insert(Block block,int numAsync) {
    synchronized (pendingReplications) {
      PendingAsyncBlockInfo found = pendingReplications.get(block);
      if (found== null) {
        found = new PendingAsyncBlockInfo(numAsync);
        pendingReplications.put(block, found);
        blockIdMapping.put(block.getBlockId(), found);
      } else {
        found.setTimeStamp();
        found.increment();
      }
      LOG.info("async_insert, " + block.getBlockId() + ", " + found.numNodes);
    }
    
  }


  void refresh(long blockID){
    PendingAsyncBlockInfo found = blockIdMapping.get(blockID);
    if (found != null){
      LOG.info("refreshing block : " + blockID);
      found.setTimeStamp();
    }
  }

  void remove(Block block) {
    synchronized (pendingReplications) {
      PendingAsyncBlockInfo found = pendingReplications.get(block);
      if (found != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Removing pending async replication for " + block);
        }
        found.decrement();
        if(found.getNumNodes() < 1){
          pendingReplications.remove(block);
          blockIdMapping.remove(block.getBlockId());
        }
        LOG.info("async_remove, " + block.getBlockId() + ", " + found.numNodes);
      }
      else
      {
        LOG.info("async_remove, " + block.getBlockId() + ",notfound");
      }
    }
  }

  void removeCompletely(Block block) {
    synchronized (pendingReplications) {
      PendingAsyncBlockInfo found = pendingReplications.get(block);
      if (found != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Removing pending async replication for " + block);
        }
        pendingReplications.remove(block);
        blockIdMapping.remove(block.getBlockId());


      }
    }
  }

  public void clear() {
    synchronized (pendingReplications) {
      pendingReplications.clear();
      timedOutItems.clear();
    }
  }

  /**
   * The total number of blocks that are undergoing replication
   */
  int size() {
    return pendingReplications.size();
  } 

  Block[] getTimedOutBlocks() {
    synchronized (timedOutItems) {
      if (timedOutItems.size() <= 0) {
        return null;
      }
      Block[] blockList = timedOutItems.toArray(
          new Block[timedOutItems.size()]);
      timedOutItems.clear();
      return blockList;
    }
  }


  static class PendingAsyncBlockInfo {
    private long timeStamp;
    private int numNodes;

    PendingAsyncBlockInfo(int numAsync) {
      this.timeStamp = System.currentTimeMillis();
      this.numNodes = numAsync;
    }

    long getTimeStamp() {
      return timeStamp;
    }

    void setTimeStamp() {
      timeStamp = System.currentTimeMillis();
    }


    public int getNumNodes(){
      return numNodes;
    }

    public void increment(){
      numNodes++;
    }
    
    public void increment(int numAsync){
      numNodes += numAsync;
    }

    public void decrement(){
      numNodes--;
    }
  }



  /*
   * A periodic thread that scans for blocks that never finished
   * their replication request.
   */
  class PendingAsyncReplicationMonitor implements Runnable {
    @Override
    public void run() {
      while (fsRunning) {
        long period = Math.min(defaultRecheckInterval, timeout);
        try {
          pendingReplicationCheck();
          Thread.sleep(period);
        } catch (InterruptedException ie) {
          if(LOG.isDebugEnabled()) {
            LOG.debug("PendingAsyncReplicationMonitor thread is interrupted.", ie);
          }
        }
      }
    }

    /**
     * Iterate through all items and detect timed-out items
     */
    void pendingReplicationCheck() {
      synchronized (pendingReplications) {
        Iterator<Map.Entry<Block, PendingAsyncBlockInfo>> iter =
            pendingReplications.entrySet().iterator();
        long now = System.currentTimeMillis();
        if(LOG.isDebugEnabled()) {
          LOG.debug("PendingAsyncReplicationMonitor checking Q");
        }
        while (iter.hasNext()) {
          Map.Entry<Block, PendingAsyncBlockInfo> entry = iter.next();
          PendingAsyncBlockInfo pendingBlock = entry.getValue();
          if (now > pendingBlock.getTimeStamp() + timeout) {
            Block block = entry.getKey();
            synchronized (timedOutItems) {
              timedOutItems.add(block);
            }
            blockIdMapping.remove(block.getBlockId());
            iter.remove();
          }
        }
      }
    }
  }

  /*
   * Shuts down the pending replication monitor thread.
   * Waits for the thread to exit.
   */
  void stop() {
    fsRunning = false;
    if(timerThread == null) return;
    timerThread.interrupt();
    try {
      timerThread.join(3000);
    } catch (InterruptedException ie) {
    }
  }

  /**
   * Iterate through all items and print them.
   */
  void metaSave(PrintWriter out) {
    synchronized (pendingReplications) {
      out.println("Metasave: Blocks being replicated: " +
          pendingReplications.size());
      Iterator<Map.Entry<Block, PendingAsyncBlockInfo>> iter =
          pendingReplications.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<Block, PendingAsyncBlockInfo> entry = iter.next();
        PendingAsyncBlockInfo pendingBlock = entry.getValue();
        Block block = entry.getKey();
        out.println(block + 
            " StartTime: " + new Time(pendingBlock.timeStamp) );
      }
    }
  }

}
