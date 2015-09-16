package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.server.protocol.PipelineFeedbackProtocol;
import org.apache.hadoop.fs.StorageType;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.Queue;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.io.*;
import java.util.*;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import org.apache.hadoop.io.nativeio.NativeIO;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;

import com.sun.jna.Native;

import sun.misc.Cleaner;

import java.lang.reflect.Field;


@SuppressWarnings("restriction")
public class FCFSManager implements PipelineFeedbackProtocol, Runnable {
  private final DataNode datanode;
  private final Configuration conf;
  public static final Log LOG = DataNode.LOG;

  private Queue<PendingForward> pendingForwards;
  private final Lock lock ;



  private AtomicInteger numBlocks;

  private int maxConcurrentReceives;

  public int bufferSize;

  private long blockBufferSize;

  StoManager ssdStoManager;
  StoManager diskStoManager;

  StoManager getStoMan(StorageType sType) throws IOException{
    switch(sType){
    case SSD: return ssdStoManager;
    case DISK: return diskStoManager;
    default:
      throw new IOException(sType.name() + " is not managed by the FCFSManager");
    }


  }

  /*
   * Memory mapped files stuff
   */
  private Map<Long,TimedBuffer> buffers;
  private Queue<UnAckBuffer> unAckBuffers;
  private Map<String,Integer> buffersCount;
  static
  {
    try
    {
      Native.register("c");
    }
    catch (NoClassDefFoundError e)
    {
      System.out.println("JNA not found. Native methods will be disabled.");
    }
    catch (UnsatisfiedLinkError e)
    {
      System.out.println("Unable to link C library. Native methods will be disabled.");
    }
    catch (NoSuchMethodError e)
    {
      System.out.println("Obsolete version of JNA present; unable to register C library. Upgrade to JNA 3.2.7 or later");
    }
  }

  private static native int mlock(ByteBuffer buf,int size);
  private static native int munlock(ByteBuffer buf,int size);

  class TimedBuffer implements Runnable{
    public long time;
    public String position;
    BlockBufferedOutputStream bout;

    TimedBuffer(BlockBufferedOutputStream _bout, String pos){
      bout = _bout;
      time = System.currentTimeMillis();
      position = pos;
    }

    @Override
    public void run() {
      NativeIO.POSIX.munmap(bout.buf);
  
    }
  }

  class UnAckBuffer{
    public long time;
    public String position;

    UnAckBuffer(String pos){
      time = System.currentTimeMillis();
      position = pos;
    }

    public long getAge(){
      return System.currentTimeMillis() - time;
    }

  }

  public void addPlaceHolder(String position){
    lock.lock();
    unAckBuffers.offer(new UnAckBuffer(position));
    Integer count = buffersCount.get(position);
    if(count == null){
      buffersCount.put(position, 1);
    }else{
      buffersCount.put(position, count + 1);
    }
    lock.unlock();
  }

  public void lockAndAdd(long blockID,BlockBufferedOutputStream bout,String position){
   try{
    NativeIO.POSIX.mlock(bout.buf,bout.getLength());
   }catch(IOException e){
     LOG.info("MERRORLOCK," + e.getMessage());
   }
    buffers.put(Long.valueOf(blockID), new TimedBuffer(bout,position));
    lock.lock();
    boolean wasThere = unAckBuffers.remove(blockID);
    Integer count = buffersCount.get(position);
    if(!wasThere){
      if(count == null){
        buffersCount.put(position, 1);
      }else{
        buffersCount.put(position, count + 1);
      }
    }
    lock.unlock();
  //DZEBUGLOG.info("BBUF," + blockID + ","+ buffers.size());
  //DZEBUGString temp = "";
  //DZEBUGfor(Entry<String, Integer> entry : buffersCount.entrySet()){
  //DZEBUG  temp += entry.getKey() + "->" + entry.getValue() + ";  ";
  //DZEBUG}
  //DZEBUGLOG.info("buffersCountA: " + temp);
  }

  public void unlockAndRemove(long blockID){
    TimedBuffer tbuf = buffers.remove(Long.valueOf(blockID));

    if(tbuf != null){
      lock.lock();
      Integer count = buffersCount.get(tbuf.position);
      if(count != null)
      {
        
        if(count > 1){
          buffersCount.put(tbuf.position, count -1);
        }
        synchronized(buffersCount){
          buffersCount.notify();
        }
      }
      lock.unlock();
      pool.submit(tbuf);
    //DZEBUGLOG.info("RBUFP," + blockID);
    }else{
    //DZEBUGLOG.info("RBUFN," + blockID);
    }
    

  }

  public BlockBufferedInputStream getBlockBufferedInputStream(long blockID){
    TimedBuffer tbuf = buffers.get(Long.valueOf(blockID));
    if(tbuf != null){
      return new BlockBufferedInputStream(this,blockID,tbuf.bout);
    }
    return null;
  }


  public void fcfsGateKeeper(String position,long blockId){
    while(true){
      Integer count = buffersCount.get(position);
      if(count  == null){
        //DZEBUGLOG.info("keepernull," + position + "," + blockId);
        break;
      }
      
      if(count < maxConcurrentReceives){
      //DZEBUGLOG.info("keeperok," + position + "," + blockId);
        break;
      }
    //DZEBUGLOG.info("gatekeeper," + position + ",keeper," + blockId);
      try{
        synchronized(buffersCount){
          buffersCount.wait(1000);
        }
      }catch(InterruptedException e){
        LOG.info("fcfsgatekeeper error " + e.getMessage());
      }

    }
  }

  class StoManager implements Runnable {
    private final FCFSManager manager;
    private ProcReader procReader;
    public Queue<PendingWrite> pendingWrites;
    private AtomicInteger numAsyncWrite;
    private int diskActivityThreshold;
    private long highActivityMean = 1;
    private long lowActivityMean = 0;
    private PositionWFQ receives;
    private long smoothedActivity=0;
    private long rawActivity=0;
    private final StorageType sType;
    private AtomicInteger foregroundRobin;
    private AtomicInteger numImmWrite;

    StoManager(FCFSManager _manager, String storageDevice, StorageType _sType) throws IOException{
      manager = _manager;
      procReader = new ProcReader(storageDevice);
      numAsyncWrite = new AtomicInteger(0);
      foregroundRobin = new AtomicInteger(0);
      numImmWrite = new AtomicInteger(0);
      pendingWrites = new LinkedBlockingQueue<PendingWrite>();
      receives = new PositionWFQ();
      sType = _sType;
    }

    @Override
    public void run() {
      while(manager.datanode.shouldRun){
        try{
          calculateSpeeds();
          processQueue();
        }catch(Exception e){
          LOG.warn("YOHWE FCFS run : " + e.toString());
          e.printStackTrace();
        }
        try{
          synchronized(this){
            this.wait(refreshInterval);
          }
        }catch(InterruptedException e){
          // ignore
        }
      }
    }

    void removePendingWrites(){
      while(!pendingWrites.isEmpty()){
        pool.submit(pendingWrites.remove());
      }
    }


    void processQueue(){
      //testing if disk activity is low
      //if( (smoothedActivity < this.diskActivityThreshold) ||  (numImmWrite.get() < 1)){
      //if(!pendingWrites.isEmpty()){
      removePendingWrites();

      lock.lock();
      while(!unAckBuffers.isEmpty()){
        if(unAckBuffers.peek().getAge() > maxUnAckTime){
          UnAckBuffer temp = unAckBuffers.poll();
          Integer count = buffersCount.get(temp.position);
          if(count != null){
             buffersCount.put(temp.position, count-1);
          }
        }else{
          break;
        }
      }
      lock.unlock();

      //testing if we don't have too many activity receives
      //      for(int i = numAsyncWrite.get() + unAckRequests.size(); i < maxConcurrentReceives; i++){
      //        if(!receives.isEmpty()){
      //          PendingReceive toReceive = receives.getReceive();
      //          LOG.info(toReceive + "," + System.currentTimeMillis());
      //          if(toReceive != null){
      //            try{
      //              LOG.info("DZUDE asking upstream to send : " + toReceive.blockID);
      //              this.manager.notifyUpStream(toReceive.sourceIP, toReceive.blockID);
      //              unAckRequests.add(new UnAckRequest());
      //              LOG.info("PENDING_RECEIVE_AGE, " +  toReceive.getAge() + "," + toReceive.flowPriority);
      //            }catch(IOException e){
      //              LOG.warn("YOHWE : notifying upstream : " + e.getMessage());
      //            }
      //          }else{
      //            LOG.warn("TORECEIVE is null");
      //          }
      //        }
      //      }

      PendingReceive toReceive;
      while( (toReceive = receives.getReceive(buffersCount,manager.maxConcurrentReceives))!=null )
      {
        LOG.info(toReceive + "," + System.currentTimeMillis());
        if(toReceive != null){
          try{
          //DZEBUGLOG.info("DZUDE asking upstream to send : " + toReceive.blockID);
            this.manager.notifyUpStream(toReceive.sourceIP, toReceive.blockID);
            manager.addPlaceHolder(toReceive.position);
          //DZEBUGLOG.info("PENDING_RECEIVE_AGE, " +  toReceive.getAge() + "," + toReceive.flowPriority);
          }catch(IOException e){
          //DZEBUGLOG.warn("YOHWE : notifying upstream : " + e.getMessage());
          }
        }

      }


    }





    private void calculateSpeeds(){
      calculateActivity();

      stat_log();
    }

    public void calculateActivity(){

      //Now using ProcReader for disk activity
      try{
        procReader.updateInfo();  
      }catch(IOException e){
        LOG.warn("Error in ProcReader : " + e);
      }
      rawActivity = procReader.getWait();
      smoothedActivity = (long)((smoothedActivity*(1-activitySmoothingExp)) + 
          (activitySmoothingExp*rawActivity));

      if( Math.abs(smoothedActivity-lowActivityMean) < Math.abs(smoothedActivity-highActivityMean)){
        lowActivityMean = (long)(lowActivityMean*(1-clusterSmoothingExp) + smoothedActivity*clusterSmoothingExp);
      }else{
        highActivityMean = (long)(highActivityMean*(1-clusterSmoothingExp) + smoothedActivity*clusterSmoothingExp);
      }

      diskActivityThreshold = (int)((lowActivityMean+highActivityMean)/2);

    }

    void stat_log(){
      if(statInterval==0){
        return;
      }
      if(System.currentTimeMillis()-lastStatLog < statInterval){
        return;
      }
      lastStatLog = System.currentTimeMillis();

      LOG.info(sType.name() + ",FCFS_STAT_DISK_THRESHOLD, " + diskActivityThreshold );
      LOG.info(sType.name() + ",FCFS_STAT_HIGH_MEAN, " + highActivityMean );
      LOG.info(sType.name() + ",FCFS_STAT_LOW_MEAN, " + lowActivityMean );
      LOG.info(sType.name() + ",FCFS_STAT_PEN_WRITE, " + pendingWrites.size());
      LOG.info(sType.name() + ",FCFS_STAT_PEN_RECEIVE, " + receives.getSize());
      LOG.info(sType.name() + ",FCFS_STAT_SMOOTHED_ACTIVITY, " + smoothedActivity);
      LOG.info(sType.name() + ",FCFS_STAT_RAW_ACTIVITY, " + rawActivity);
      LOG.info(sType.name() + ",FCFS_STAT_NUM_ASYNC_WRITE, " + numAsyncWrite.get());
      LOG.info(sType.name() + ",FCFS_STAT_UNACK_REQUESTS, " + unAckBuffers.size());
      LOG.info(sType.name() + ",FCFS_STAT_ACTIVITY_DIFFERENCE, " + (smoothedActivity-diskActivityThreshold));
      LOG.info(sType.name() + ",FCFS_STAT_READ_THROUGHPUT, " + procReader.getReadThroughput());
      LOG.info(sType.name() + ",FCFS_STAT_WRITE_THROUGHPUT, " + procReader.getWriteThroughput());
      LOG.info(sType.name() + ",FCFS_STAT_READ_TOTAL, " + procReader.getReadTotal());
      LOG.info(sType.name() + ",FCFS_STAT_WRITE_TOTAL, " + procReader.getWriteTotal());
      LOG.info(sType.name() + ",FCFS_STAT_IMM_WRITE, " + numImmWrite);
      LOG.info(sType.name() + ",FCFS_STAT_NUM_POS_QUEUE, " + receives.queues.size());
      LOG.info("FCFS_STAT_BUFFERS_SIZE,"+ buffers.size());
      Integer count;
      count = buffersCount.get("0");
      if(count != null){
        LOG.info("FCFS_STAT_BUFFERS_POS,0,"+ count );
      }
      count = buffersCount.get("1");
      if(count != null){
        LOG.info("FCFS_STAT_BUFFERS_POS,1,"+ count );
      }
      count = buffersCount.get("2");
      if(count != null){
        LOG.info("FCFS_STAT_BUFFERS_POS,2,"+ count );
      }

    }

    public void addImmWrite(){
      numImmWrite.getAndIncrement();
      foregroundRobin.getAndIncrement();
//      if(foregroundRobin.get()>=19){
//        foregroundRobin.set(0);
//        if(!pendingWrites.isEmpty()){
//          removePendingWrite();
//        }
//      }
    }

    public void removeImmWrite(){
      numImmWrite.getAndDecrement();
    }

  }


  static int rpcPort = DFSConfigKeys.FCFS_RPC_DEFAULT_PORT;


  private boolean prioritizeEarlierReplicas = DFSConfigKeys.FCFS_PRIORITIZE_EARLIER_REPLICAS_DEFAULT;

  private final ExecutorService pool;


  //Interval between logs and stat refreshing

  private long refreshInterval = DFSConfigKeys.FCFS_REFRESH_INTERVAL_DEFAULT;
  private long statInterval = DFSConfigKeys.FCFS_STAT_INTERVAL_DEFAULT;
  private long maxUnAckTime ;
  private long lastStatLog = 0;

  //activity threshold and mention settings

  private float activitySmoothingExp;
  private float clusterSmoothingExp;

  private float[] positionPriority;

  public void incBlockCount(){
    numBlocks.getAndIncrement();
  }
  public void addAsyncWrite(StorageType sType) throws IOException{
    getStoMan(sType).numAsyncWrite.getAndIncrement();
  }

  public void removeAsyncWrite(StorageType sType) throws IOException{
    getStoMan(sType).numAsyncWrite.getAndDecrement();
  }

  public void addImmWrite(StorageType sType) throws IOException{
    getStoMan(sType).addImmWrite();
  }

  public void removeImmWrite(StorageType sType) throws IOException{
    getStoMan(sType).removeImmWrite();
  }

  public long getBlockBufferSize(){
    return blockBufferSize;
  }

  public FCFSManager(Configuration conf, DataNode datanode) throws IOException{
    this.datanode = datanode;
    this.conf = conf;
    lock = new ReentrantLock();
    pendingForwards = new PriorityBlockingQueue<PendingForward>();
    buffers= new ConcurrentHashMap<Long,TimedBuffer>();
    unAckBuffers = new LinkedBlockingQueue<UnAckBuffer>();
    buffersCount = new ConcurrentHashMap<String,Integer>();
    maxConcurrentReceives = conf.getInt(DFSConfigKeys.FCFS_MAX_CONCURRENT_RECEIVES_KEY,
        DFSConfigKeys.FCFS_MAX_CONCURRENT_RECEIVES_DEFAULT);
    bufferSize = conf.getInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,(int)DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT);
    activitySmoothingExp= conf.getFloat(DFSConfigKeys.FCFS_ACTIVITY_SMOOTHING_EXP_KEY,
        DFSConfigKeys.FCFS_ACTIVITY_SMOOTHING_EXP_DEFAULT);
    clusterSmoothingExp= conf.getFloat(DFSConfigKeys.FCFS_CLUSTER_SMOOTHING_EXP_KEY,
        DFSConfigKeys.FCFS_CLUSTER_SMOOTHING_EXP_DEFAULT);
    prioritizeEarlierReplicas = conf.getBoolean(DFSConfigKeys.FCFS_PRIORITIZE_EARLIER_REPLICAS_KEY, 
        DFSConfigKeys.FCFS_PRIORITIZE_EARLIER_REPLICAS_DEFAULT);
    refreshInterval = conf.getLong(DFSConfigKeys.FCFS_REFRESH_INTERVAL_KEY,
        DFSConfigKeys.FCFS_REFRESH_INTERVAL_DEFAULT);
    statInterval = conf.getLong(DFSConfigKeys.FCFS_STAT_INTERVAL_KEY,
        DFSConfigKeys.FCFS_STAT_INTERVAL_DEFAULT);
    blockBufferSize  = conf.getLong(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT) ;
    maxUnAckTime  = conf.getLong(DFSConfigKeys.FCFS_MAX_UNACK_TIME_KEY,
        DFSConfigKeys.FCFS_MAX_UNACK_TIME_DEFAULT);
    positionPriority = PFPUtils.colonsplit(conf.getStrings(DFSConfigKeys.FCFS_POSITION_PRIORITY_KEY,DFSConfigKeys.FCFS_POSITION_PRIORITY_DEFAULT)[0]);

    numBlocks = new AtomicInteger(0);



    pool = Executors.newFixedThreadPool(16);
    //pool = Executors.newFixedThreadPool(5);

    //pool = Executors.newSingleThreadExecutor();
    try{
      ssdStoManager = new StoManager(this,"sdb",StorageType.SSD);
      diskStoManager = new StoManager(this,"sdc",StorageType.DISK);

    }catch(Exception e){
      LOG.warn(e.getMessage());
    }  

    pool.submit(ssdStoManager);
    pool.submit(diskStoManager);
    pool.submit(this);



    try{
      this.startRpcServer();
    }catch(IOException e){
      LOG.warn("Could not start FCFS Manager Rpc Server : " + e.getMessage());
    }

  }



  class PendingWrite implements Comparable<PendingWrite>,Runnable{
    private final BlockReceiver blockReceiver;
    private final PendingForward toForward;
    private final long timeCreated;
    private final StorageType sType;

    private final FCFSManager manager;

    public PendingWrite(BlockReceiver blockReceiver, FCFSManager manager, PendingForward toForward, StorageType _sType){
      this.timeCreated = System.currentTimeMillis();
      this.blockReceiver = blockReceiver;
      this.manager = manager;
      this.toForward = toForward;
      this.sType = _sType;
    }



    @Override
    public int compareTo(PendingWrite other) {
      return  Long.valueOf(timeCreated).compareTo(other.timeCreated);
    }

    public long getAge(){
      return System.currentTimeMillis() - timeCreated;
    }

    @Override
    /*
     * Called the delayed close function for a block receiver to flush buffer contents
     * to disk and finalize the block
     */
    public void run() {
      try{
        try{
        //DZEBUG  LOG.info("FCFS_BCOUNT, " + blockReceiver.getCount());
          blockReceiver.delayedClose();
        }catch(Exception e){
          LOG.warn("PendingWriteException : " + e.toString());
        }
        if(this.toForward.hasTargets()){
          this.manager.addPendingForward(this.toForward);
        } else{
        //DZEBUG  LOG.info("REMOVING," + this.blockReceiver.getBlockId());
          this.manager.unlockAndRemove(this.blockReceiver.getBlockId());
        }
      }finally{
        try{
          this.manager.removeAsyncWrite(sType);
        }catch(IOException e){
          LOG.warn(e.toString());
        }
        this.manager.resume();
      }
    }

  }



  class PendingForward implements Comparable<PendingForward>{
    private final ExtendedBlock block;
    private final DatanodeInfo[] targets;
    private final StorageType[] targetStorageTypes;
    private long timeCreated;
    private final DataNode datanode;
    private float replicationPriority;
    private String flowName;
    private int numImmediate;
    private final int pipelineSize;

    public PendingForward(ExtendedBlock block,DatanodeInfo[] targets,StorageType[] targetStorageTypes, DataNode datanode, float repPriority, String flowName,int numImmediate,int pipelineSize){
      this.timeCreated = System.currentTimeMillis();
      this.block = block;
      this.targets = targets;
      this.targetStorageTypes =  targetStorageTypes;
      this.datanode = datanode;
      this.replicationPriority = repPriority;
      this.flowName = flowName;
      this.numImmediate = numImmediate;
      this.pipelineSize = pipelineSize;

    } 


    @Override
    public int compareTo(PendingForward other) {
      return  Long.valueOf(timeCreated).compareTo(other.timeCreated);
    }

    public long getAge(){
      return System.currentTimeMillis() - timeCreated;
    }

    public void resetAge(){
      this.timeCreated = System.currentTimeMillis();
    }

    public void forward(){
      try{
      //DZEBUGLOG.info("DZINEX : ppSize : " + pipelineSize);
        datanode.FCFStransferBlock(block,targets,targetStorageTypes,replicationPriority, flowName,numImmediate,pipelineSize);
      }catch(Exception e){
        LOG.warn(e.toString());
      }
    }

    public boolean hasTargets(){
      if(targets == null){
        return false;
      }
      return targets.length > 0;
    }
  }


  void addPendingWrite(BlockReceiver receiver,ExtendedBlock block, DatanodeInfo[] targets,StorageType[] targetStorageTypes, 
      float replicationPriority, String flowName,int numImmediate,int pipelineSize,StorageType sType) throws IOException{

  //DZEBUGLOG.info("DZUDE : Adding pending write 1");
    PendingForward toForward = null;
    toForward = new PendingForward(block,targets,targetStorageTypes,datanode,replicationPriority,flowName,numImmediate,pipelineSize);

    this.getStoMan(sType).pendingWrites.add(new PendingWrite(receiver,this,toForward,sType));
  }


  public void addPendingForward(ExtendedBlock block, DatanodeInfo[] targets,StorageType[] targetStorageTypes, float replicationPriority, String flowName,int numImmediate,int pipelineSize){
    pendingForwards.add(new PendingForward(block,targets,targetStorageTypes,datanode,replicationPriority, flowName,numImmediate,pipelineSize));
    int pipelinePosition = (pipelineSize - targets.length)-1;
  //DZEBUGLOG.info("DZUDE : Adding pending forward 1");
    try{
      String message = PFPUtils.merge(new String[]{
          this.datanode.getDatanodeId().getIpAddr(),
          Long.valueOf(block.getBlockId()).toString(),
          Long.valueOf(block.getNumBytes()).toString(),
          Float.valueOf(replicationPriority).toString(),
          flowName,
          (this.prioritizeEarlierReplicas?Integer.valueOf(pipelinePosition+1).toString():"1"),
          targetStorageTypes[0].name()
      });
      this.notifyDownStream(targets[0],message);
    }catch(IOException e){
      LOG.warn("Error : Adding pending forward 1: " + e.getMessage());
    }
  }

  private void addPendingForward(PendingForward toForward){
    toForward.resetAge();
    pendingForwards.add(toForward);
    int pipelinePosition = (toForward.pipelineSize - toForward.targets.length)-1;
  //DZEBUGLOG.info("DZUDE : Adding pending forward 2");
    try{
      String message = PFPUtils.merge(new String[]{
          this.datanode.getDatanodeId().getIpAddr(),
          Long.valueOf(toForward.block.getBlockId()).toString(),
          Long.valueOf(toForward.block.getNumBytes()).toString(),
          Float.valueOf(toForward.replicationPriority).toString(),
          toForward.flowName,
          (this.prioritizeEarlierReplicas?Integer.valueOf(pipelinePosition+1).toString():"1"),
          toForward.targetStorageTypes[0].name()
      });
      this.notifyDownStream(toForward.targets[0],message);
    }catch(IOException e){
      LOG.warn("Error : Adding pending forward 2: " + e.getMessage());
    }
  }

  void removePendingForward(boolean sepThread){
    pendingForwards.remove();
  }


  public boolean shouldWriteDirect(int position,int numImmediate,String flowName,StorageType sType) throws IOException{
    if(!isAsyncWrite(position,numImmediate,flowName)){
      return true;
    }else{
      addAsyncWrite(sType);
      return false;
    }
  }

  boolean shouldSegment(int position,int numImmediate,int pipelineSize,String flowName){

    //always return false for last datanode in pipeline
    if(position+1 >= pipelineSize){
      return false;
    }

    //segment is the next datanode should be written to asynchronously
    return isAsyncWrite(position+1,numImmediate,flowName);
  }

  synchronized boolean isAsyncWrite(int position,int numImmediate,String flowName){
    if(!flowName.contains("attempt")){
      return false;
    }

    //numImmediateWrites < 0 indicates no segmentation
    if(numImmediate < 0){
      return false;
    }

    if(position+1 > numImmediate){
      return true;
    }else{
      return false;
    }

  }








  void stat_log(){
    if(statInterval==0){
      return;
    }
    if(System.currentTimeMillis()-lastStatLog < statInterval){
      return;
    }
    lastStatLog = System.currentTimeMillis();

    LOG.info("GLO" + "FCFS_STAT_PEN_FORWARD, " + pendingForwards.size());
    LOG.info("GLO" + "FCFS_STAT_BLOCK_COUNT, " + numBlocks.get());

  }



  public void run() {
    while(datanode.shouldRun){
      try{
        synchronized(this){
          this.wait(refreshInterval);
        }
      }catch(InterruptedException e){
        // ignore
      }
    }

  }

  synchronized public void resume(){
    this.notify();
  }

  void kill() {
    if (this.RpcServer != null) {
      this.RpcServer.stop();
      LOG.info("DZUDE : RPC server stopped");
    }
    pool.shutdown();
    assert datanode.shouldRun == false :
      "shoudRun should be set to false before killing";
  }



  public RPC.Server RpcServer;

  private void startRpcServer() throws IOException{

    InetSocketAddress RpcAddr = NetUtils.createSocketAddr(this.conf.get(
        DFSConfigKeys.FCFS_RPC_ADDRESS_KEY,
        DFSConfigKeys.FCFS_RPC_ADDRESS_DEFAULT));

    RpcServer = new RPC.Builder(conf)
    .setProtocol(PipelineFeedbackProtocol.class)
    .setInstance(this)
    .setBindAddress(RpcAddr.getHostName())
    .setPort(RpcAddr.getPort())
    .setNumHandlers(conf.getInt(DFS_DATANODE_HANDLER_COUNT_KEY,
        DFS_DATANODE_HANDLER_COUNT_DEFAULT))
        .setVerbose(false)
        .setSecretManager(datanode.blockPoolTokenSecretManager).build();

    RpcServer.start();
    LOG.info("DZUDE : RPC server started");

  }

  @Override
  public String informUpStream(String message) throws IOException {
  //DZEBUGLOG.info("DZUDE upstream node received request for Block : " + message);
    String[] parts= PFPUtils.split(message);
    if(parts[0].equals("remove")){
    //DZEBUGLOG.info("DZUDE removing block " + parts[1] + " from pendingForwards");
      this.removeFromPendingForward(parts[1]);
      return ("removed : " + parts[1]);
    }
    else{
      if(sendDownStream(message)){
        return (parts[0] + " being sent by upstream node");
      }else
      {
        return (parts[0] + " not found in pendingForwards");
      }
    }

  }

  public boolean sendDownStream(String blockID){
  //DZEBUGLOG.info("AMDG : SDS : REQUEST : " + blockID);
  //DZEBUG LOG.info("AMDG : SDS : SIZE : " + pendingForwards.size());
    Iterator<PendingForward> it = pendingForwards.iterator();
    boolean isFound = false;
    while(it.hasNext() && !isFound){
      PendingForward current = it.next();
      if(blockID.equals(Long.valueOf(current.block.getBlockId()).toString())){
        isFound = true;
      //DZEBUG LOG.info("AMDG : SDS : HANDE :" + current.block.getBlockId());
        it.remove();
        current.forward();  
      }

    }
    return isFound;
  }


  public void removeFromPendingForward(String blockID){
    Iterator<PendingForward> it = pendingForwards.iterator();
    boolean isFound = false;
    PendingForward current ;
    while(it.hasNext() && !isFound){
      current= it.next();
      if(blockID.equals(Long.valueOf(current.block.getBlockId()).toString())){
        isFound = true;
      //DZEBUGLOG.info("AMDG : RFPF : FOUND :" + current.block.getBlockId());
        it.remove();
      }

    }
  }

  public float getPriority(String position){
    int pos = Integer.valueOf(position).intValue();
    if(pos<=positionPriority.length){
      return positionPriority[pos-1];
    }else{
      return positionPriority[positionPriority.length-1];
    }
  }

  public void removeFromPendingReceives(long blockID, String flowName,int position,StorageType sType) throws IOException{
    PendingReceive removed = getStoMan(sType).receives.remove(Long.valueOf(blockID).toString(),flowName,Integer.valueOf(position).toString());
    if(removed != null){
      try {
        this.notifyUpStream(removed.sourceIP, "remove," + removed.blockID);
      } catch (IOException e) {
        LOG.warn("YOHWE Exception: " + e.getMessage());
      }
    }
  }

  @Override
  public String informDownStream(String message) throws IOException{
  //DZEBUGLOG.info("DZUDE downstream node got : " + message);
    String[] parts = PFPUtils.split(message);
    StorageType sType = StorageType.parseStorageType(parts[6]);

    getStoMan(sType).receives.addReceive(new PendingReceive(message,getPriority(parts[5])));
    return message;
  }

  public static PipelineFeedbackProtocol createPipelineFeedbackProtocolProxy(
      String datanodeIP, final Configuration conf, final int socketTimeout) throws IOException{
    final String dnAddr;
    //Get address to connect to server
    dnAddr = datanodeIP + ":" + FCFSManager.rpcPort;  
    //Create socket
    final InetSocketAddress addr = NetUtils.createSocketAddr(dnAddr);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to datanode " + dnAddr + " addr=" + addr);
    }
    final UserGroupInformation loginUgi = UserGroupInformation.getLoginUser();
    try{
      return loginUgi
          .doAs(new PrivilegedExceptionAction<PipelineFeedbackProtocol>(){
            @Override
            public PipelineFeedbackProtocol run() throws IOException{
              return RPC.getProxy(PipelineFeedbackProtocol.class,
                  RPC.getProtocolVersion(PipelineFeedbackProtocol.class), addr, loginUgi, conf,
                  NetUtils.getDefaultSocketFactory(conf), socketTimeout);
            }
          });
    }catch (InterruptedException ie) {
      throw new IOException(ie.getMessage());
    }

  }

  private void notifyDownStream(DatanodeInfo dnInfo,String message) throws IOException{
    PipelineFeedbackProtocol downstream;
    try{
      downstream = createPipelineFeedbackProtocolProxy(dnInfo.getIpAddr(), conf, 
          this.datanode.getDnConf().socketTimeout);
      try{
        LOG.info("DZUDE response from downstream is: " +  downstream.informDownStream(message));
      } 
      catch(RemoteException re){
        LOG.warn("Failed to notify downstream node Remote : " + re.getMessage());
      }

    }catch(IOException e) {
      LOG.warn("Failed to notify downstream node : " + e.getMessage());
    } 
  }

  private void notifyUpStream(String datanodeIP,String message) throws IOException{

    PipelineFeedbackProtocol upstream;
    try{
      upstream = createPipelineFeedbackProtocolProxy(datanodeIP, conf, 
          this.datanode.getDnConf().socketTimeout);
      try{
       LOG.info("Zvaita response from upstream is : " +  upstream.informUpStream(message));
      } 
      catch(RemoteException re){
        LOG.warn("Failed to notify downstream node Remote : " + re.getMessage());
      }

    }catch(IOException e) {
      LOG.warn("Failed to notify downstream node : " + e.getMessage());
    } 

  }


  public static class PFPUtils{
    public static String merge(String[] strings){
      String result = "";
      for(int i = 0; i < strings.length ; i++){
        result += strings[i];
        if(i+1 < strings.length){
          result += ",";
        }
      }
      return result;
    }


    public static String[] split(String message){
      return message.split(",");
    }

    public static float[] colonsplit(String message){
      String[] prios = message.split(":");
      float[] results = new float[prios.length];
      for(int i = 0; i < prios.length; i++){
        results[i] = Float.valueOf(prios[i]).floatValue();
      }
      return results;
    }

  }





}
