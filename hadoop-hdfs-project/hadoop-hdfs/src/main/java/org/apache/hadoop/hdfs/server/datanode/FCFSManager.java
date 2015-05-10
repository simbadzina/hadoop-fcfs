package org.apache.hadoop.hdfs.server.datanode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_DEFAULT;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HANDLER_COUNT_KEY;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocolPB.ClientDatanodeProtocolPB;
import org.apache.hadoop.hdfs.protocolPB.InterDatanodeProtocolPB;
import org.apache.hadoop.hdfs.server.protocol.PipelineFeedbackProtocol;

import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.*;

import org.apache.hadoop.util.Daemon;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;
import java.text.*;
import java.lang.*;
import java.net.InetSocketAddress;


import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

import java.security.PrivilegedExceptionAction;

import org.apache.commons.lang.StringUtils;

public class FCFSManager {
  private final Datanode datanode;
  private final Configuration conf;
  public static final Log LOG = DataNode.LOG;

  private Queue<PendingWrite> pendingWrites;
  private Queue<PendingForward> pendingForwards;
  private Queue<UnAckRequest> unAckRequests;

  private AtomicInteger numImmWrite;
  private AtomicInteger numPenWrite;
  private int diskActivityThreshold;
  private int maxBufferedBlocks;
  private long maxUnAckTime;
  private long maxTime;

  public int bufferSize;

  private long statRefreshInterval = 0;


  private long diskActivity = 0;

  private long pendFlushOld = 0;
  private AtomicLong pendFlushTotal;
  private long pendFlushSpeed = 0;

  private long pendForwardOld = 0;
  private AtomicLong pendForwardTotal;
  private long pendForwardSpeed = 0;

  private long lastTime = 0;
  private boolean flushInSeparateThread = false;




  static int rpcPort = DFSConfigKeys.FCFS_RPC_DEFAULT_PORT;

  private WFQScheduler receives;
  private boolean prioritizeEarlierReplicas = DFSConfigKeys.FCFS_PRIORITIZE_EARLIER_REPLICAS_DEFAULT;
  private int logIt = 0;
  private int logItMod =1;
  private final ExecutorService pool;


  //Interval between logs and stat refreshing
  private long lastLog =0;
  private long logInterval = DFSConfigKeys.FCFS_LOG_INTERVAL_DEFAULT;
  private long statInterval = DFSConfigKeys.FCFS_STAT_INTERVAL_DEFAULT;


  //activity threshold and mention settings
  private ProcReader reader;
  private long smoothedActivity=0;
  private long rawActivity=0;

  private float activitySmoothingExp;
  private float clusterSmoothingExp;
  private long highActivityMean = 0;
  private long lowActivityMean = 1;
  
  public void decrementNumPenWrite(){
    numPenWrite.getAndDecrement();
  }
  
  synchronized void errorWithIncomingPendingWrite(){
    numPenWrite.getAndDecrement();
  }

  public FCFSManager(Configuration conf, DataNode datanode){
    this.datanode = datanode;
    this.conf = conf;

    pendingWrites = new PriorityBlockingQueue<PendingWrite>();
    pendingForwards = new PriorityBlockingQueue<PendingForward>();
    //blocksPending = new PriorityBlockingQueue<ExtendedBlock>();

    maxBufferedBlocks = conf.getInt(DFSConfigKeys.FCFS_MAX_BUFFERED_KEY,
        DFSConfigKeys.FCFS_MAX_BUFFERED_DEFAULT);
    maxTime = conf.getLong(DFSConfigKeys.FCFS_REPLICATION_TIMEOUT_KEY,
        DFSConfigKeys.FCFS_REPLICATION_TIMEOUT_DEFAULT);

    bufferSize = conf.getInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY,DFSConfigKeys.DFS_BLOCK_SIZE_DEFAULT) + 1024*1024;

    activitySmoothingExp= conf.getFloat(DFSConfigKeys.FCFS_ACTIVITY_SMOOTHING_EXP_KEY,
        DFSConfigKeys.FCFS_ACTIVITY_SMOOTHING_EXP_DEFAULT);
    clusterSmoothingExp= conf.getFloat(DFSConfigKeys.FCFS_CLUSTER_SMOOTHING_EXP_KEY,
        DFSConfigKeys.FCFS_CLUSTER_SMOOTHING_EXP_DEFAULT);


    prioritizeEarlierReplicas = conf.getBool(DFSConfigKeys.FCFS_PRIORITIZE_EARLIER_REPLICAS_KEY, 
        DFSConfigKeys.FCFS_PRIORITIZE_EARLIER_REPLICAS_DEFAULT);

    logInterval = conf.getLong(DFSConfigKeys.FCFS_LOG_INTERVAL_KEY,
        DFSConfigKeys.FCFS_LOG_INTERVAL_DEFAULT);
    statInterval = conf.getLong(DFSConfigKeys.FCFS_STAT_INTERVAL_KEY,
        DFSConfigKeys.FCFS_STAT_INTERVAL_DEFAULT);


    numImmWrite = new AtomicInteger(0);
    numPenWrite = new AtomicInteger(0);
    pendFlushTotal = new AtomicLong(0);
    pendForwardTotal = new AtomicLong(0);


    receives = new WFQScheduler(this);
    //refreshingThread = new Daemon(receives);
    //refreshingThread.start();

    pool = Executors.newSingleThreadExecutor();
    try{
      reader = new ProcReader();
    }catch(FileNotFoundException e){
      LOG.warn("Cannot find diskstats file");
    }   

   
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
    private final int pipelineSize;

    long bCount;
    private final FCFSManager manager;

    public PendingWrite(BlockReceiver blockReceiver, FCFSManager manager, PendingForward toForward,int pipelineSize){
      this.timeCreated = System.currentTimeMillis();
      this.blockReceiver = blockReceiver;
      this.manager = manager;
      this.toForward = toForward;
      this.pipelineSize = pipelineSize;
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
        LOG.info("PENDING_WRITE_AGE, " +  this.getAge());
        try{
          blockReceiver.delayedClose();
        }catch(Exception e){
          LOG.warn("DZED exception : " + e.toString());
        }
        if(this.toForward != null){
          this.manager.addPendingForward(this.toForward);
        }
        this.manager.pendFlushTotal.getAndAdd(bCount);
      }finally{
        this.manager.resume();
      }
    }

  }
  
  
  
  
  class PendingForward implements Comparable<PendingForward>,Runnable{
    private final ExtendedBlock block;
    private final DatanodeInfo[] targets;
    private long timeCreated;
    private final DataNode datanode;
    private final FCFSManager manager;
    private float replicationPriority;
    private String flowName;
    private final int pipelineSize;

    public PendingForward(ExtendedBlock block,DatanodeInfo[] targets, DataNode datanode, FCFSManager  manager, float repPriority, String flowName,int pipelineSize){
      this.timeCreated = System.currentTimeMillis();
      this.block = block;
      this.targets = targets;
      this.datanode = datanode;
      this.manager = manager;
      this.replicationPriority = repPriority;
      this.flowName = flowName;
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

    @Override
    public void run() {
      long bSize = block.getNumBytes();
      try{
        LOG.info("DZINEX : ppSize : " + pipelineSize);
        datanode.transferBlock(block,targets,replicationPriority, flowName,pipelineSize);
      }catch(Exception e){
        LOG.warn(e.toString());
      }
      manager.pendForwardTotal.getAndAdd(bSize);

    }

  }
  
  
  void addPendingWrite(BlockReceiver receiver,ExtendedBlock block, DatanodeInfo[] targets, float repPriority, String flowName,int pipelineSize){

    LOG.warn("DZUDE : Adding pending write 1");
    PendingForward toForward = null;
    if(block!=null && targets !=null){
      toForward = new PendingForward(block,targets,datanode,this,repPriority,flowName,pipelineSize);
    }
    if(receiver==null){
      LOG.warn("SILO : receiver is null");
    }

    pendingWrites.add(new PendingWrite(receiver,this,toForward,pipelineSize));
  }

  void removePendingWrite(boolean sepThread){
    if(!pendingWrites.isEmpty()){
      pool.submit(pendingWrites.remove());
      /*
      if(sepThread){

        pool.submit(pendingWrites.remove());
        //Thread t = new Thread(pendingWrites.remove());
        //t.start();
      } else{
        pendingWrites.remove().run();
      }
       */
      totalRepBlocksWritten.getAndIncrement();
    }
  }


  void addPendingForward(ExtendedBlock block, DatanodeInfo[] targets, float replicationPriority, String flowName,int pipelineSize){
    pendingForwards.add(new PendingForward(block,targets,datanode,this,replicationPriority, flowName,pipelineSize));
    numPenForward.getAndIncrement();
    int pipelinePosition = (pipelineSize - targets.length)-1;
    LOG.warn("DZUDE : Adding pending forward 1");
    try{
      String message = PFPUtils.merge(new String[]{
          this.datanode.getDatanodeId().getIpAddr(),
          Long.valueOf(block.getBlockId()).toString(),
          Long.valueOf(block.getNumBytes()).toString(),
          Float.valueOf(replicationPriority).toString(),
          flowName,
          Integer.valueOf(pipelinePosition+1).toString(),
          block.getBlockPoolId()
      });
      this.notifyDownStream(targets[0],message);
    }catch(IOException e){
      LOG.warn("Error : Adding pending forward 1: " + e.getMessage());
    }
  }

  void addPendingForward(PendingForward toForward){
    toForward.resetAge();
    pendingForwards.add(toForward);
    int pipelinePosition = (toForward.pipelineSize - toForward.targets.length)-1;
    LOG.warn("DZUDE : Adding pending forward 2");
    try{
      String message = PFPUtils.merge(new String[]{
          this.datanode.getDatanodeId().getIpAddr(),
          Long.valueOf(toForward.block.getBlockId()).toString(),
          Long.valueOf(toForward.block.getNumBytes()).toString(),
          Float.valueOf(toForward.replicationPriority).toString(),
          toForward.flowName,
          Integer.valueOf(pipelinePosition+1).toString(),
          toForward.block.getBlockPoolId()
      });
      this.notifyDownStream(toForward.targets[0],message);
    }catch(IOException e){
      LOG.warn("Error : Adding pending forward 2: " + e.getMessage());
    }
  }

  void removePendingForward(boolean sepThread){
    if(sepThread){
      if(!pendingForwards.isEmpty()){
        Thread t = new Thread(pendingForwards.remove());
        t.start();
      }
    } else{
      pendingForwards.remove().run();
    }
  }
 
  
  boolean shouldSegment(int position,int numImmediateWrites){
    if(numImmediateWrites < 1){
      return false;
    }
    
    if(position+1 >= numImmediateWrites){
      return true;
    }else{
      return false;
    }
  }

  synchronized boolean shouldWriteDirect(int position,int numImmediateWrites){
    if(numImmediateWrites < 1){
      return true;
    }
    
    if(position+1 > numImmediateWrites){
      numPenWrite.getAndIncrement();
      return false;
    }else{
      return true;
    }
   
  }
 
  void processQueue(){
    
    if( (smoothedActivity < this.diskActivityThreshold) ||  (numImmWrite.get() < 1)){
      if(!pendingWrites.isEmpty()){
        removePendingWrite(flushInSeparateThread);
      }
    }


    //If oldest pending write is too old, flush it
    if(!pendingWrites.isEmpty()){
      //LOG.info("DEBUG getAge : " + pendingWrites.peek().getAge());
      if(pendingWrites.peek().getAge() > maxTime){
      //  removePendingWrite(true);
      }
    }


    for(int i = numPenWrite.get() ; i < maxBufferedBlocks; i++){
      if(!receives.isEmpty()){
        PendingReceive toReceive = receives.getReceive();
        if(toReceive != null){
          try{
            LOG.info("DZUDE asking upstream to send : " + toReceive.blockID);
            this.notifyUpStream(toReceive.sourceIP, toReceive.blockID);
            LOG.info("PENDING_RECEIVE_AGE, " +  toReceive.getAge() + "," + toReceive.replicationPriority);
          }catch(IOException e){
            LOG.warn("YOHWE : notifying upstream : " + e.getMessage());
          }
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
    rawActivity = reader.getWait();
    }catch(IOException e){
      LOG.warn("Error in ProcReader : " + e);
    }
    smoothedActivity = (long)((smoothedActivity*(1-smoothedActivity)) + 
        (activitySmoothingExp*rawActivity));
    
    if( Math.abs(smoothedActivity-lowActivityMean) < Math.abs(smoothedActivity-highActivityMean)){
       lowActivityMean = (long)(lowActivityMean*(1-clusterSmoothingExp) + smoothedActivity*clusterSmoothingExp);
    }else{
      highActivityMean = (long)(highActivityMean*(1-clusterSmoothingExp) + smoothedActivity*clusterSmoothingExp);
    }
    
     diskActivityThreshold = (int)((lowActivityMean+highActivityMean)/2);
    

  }

  public void run() {
    while(datanode.shouldRun){
      try{
        calculateSpeeds();
        processQueue();
      }catch(Exception e){
        LOG.warn("YOHWE drm run : " + e.toString());
      }
      try{
        synchronized(this){
          this.wait(pendingFlushSeparation);
        }
        //Thread.sleep(pendingFlushSeparation);
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
    receives.fsRunning = false;
    assert datanode.shouldRun == false :
      "shoudRun should be set to false before killing";
  }
  
  
  
  public RPC.Server RpcServer;

  private void startRpcServer() throws IOException{

    InetSocketAddress RpcAddr = NetUtils.createSocketAddr(this.conf.get(
        DFSConfigKeys.DFS_DELAYED_REPLICATION_RPC_ADDRESS_KEY,
        DFSConfigKeys.DFS_DELAYED_REPLICATION_ADDRESS_DEFAULT));

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
    LOG.info("DZUDE upstream node received request for Block : " + message);
    String[] parts= PFPUtils.split(message);
    if(parts[0].equals("remove")){
      LOG.info("DZUDE removing block " + parts[1] + " from pendingForwards");
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
    LOG.info("AMDG : SDS : REQUEST : " + blockID);
    LOG.info("AMDG : SDS : SIZE : " + pendingForwards.size());
    Iterator<PendingForward> it = pendingForwards.iterator();
    boolean isFound = false;
    while(it.hasNext() && !isFound){
      PendingForward current = it.next();
      //LOG.info("AMDG : SDS :" + current.block.getBlockId());
      if(blockID.equals(Long.valueOf(current.block.getBlockId()).toString())){
        isFound = true;
        LOG.info("AMDG : SDS : HANDE :" + current.block.getBlockId());
        it.remove();
        Thread t = new Thread(current);
        t.start();
        numPenForward.getAndDecrement();
      }

    }
    return isFound;
  }


  public void removeFromPendingForward(String blockID){
    rfpfNanos = System.nanoTime();
    Iterator<PendingForward> it = pendingForwards.iterator();
    boolean isFound = false;
    PendingForward current ;
    while(it.hasNext() && !isFound){
      current= it.next();
      //LOG.info("AMDG : RFPF : " + current.block.getBlockId());
      if(blockID.equals(Long.valueOf(current.block.getBlockId()).toString())){
        isFound = true;
        LOG.info("AMDG : RFPF : FOUND :" + current.block.getBlockId());
        it.remove();
        numPenForward.getAndDecrement();
      }

    }
    LOG.info("STAT_NANOS_RFPF, " + (System.nanoTime()-rfpfNanos));
  }


  public void removeFromPendingReceives(long blockID, String flowName){
    rfprNanos = System.nanoTime();
    receives.remove(blockID,flowName);
    LOG.info("STAT_NANOS_RFPR, " + (System.nanoTime()-rfprNanos));
  }

  @Override
  public String informDownStream(String message) {
    LOG.info("DZUDE downstream node got : " + message);
    receives.addReceive(new PendingReceive(message));
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
      downstream = this.createPipelineFeedbackProtocolProxy(dnInfo.getIpAddr(), conf, 
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
      upstream = this.createPipelineFeedbackProtocolProxy(datanodeIP, conf, 
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

  }

  public class PendingReceive implements Comparable<PendingReceive>{
    private String blockID;
    private String sourceIP;
    public long blockSize;
    public float replicationPriority;
    public String flowName;
    private final long timeCreated;
    public long startTime;
    public long finishTime;
    public int pipelinePosition;
    private String blockPoolId;

    public PendingReceive(String message){
      String[] parts = PFPUtils.split(message);
      sourceIP = parts[0];
      blockID = parts[1];
      blockSize = Long.valueOf(parts[2]).longValue();
      replicationPriority = Float.valueOf(parts[3]).floatValue();
      flowName = parts[4];
      pipelinePosition = Integer.valueOf(parts[5]);
      blockPoolId = parts[6];
      timeCreated = System.currentTimeMillis();
    }


    @Override
    public int compareTo(PendingReceive other) {
      return  Long.valueOf(timeCreated).compareTo(other.timeCreated);
    }

    public long getAge(){
      return System.currentTimeMillis() - timeCreated;
    }

  }

  public class UnAckRequest implements Comparable<UnAckRequest>{
    private final long timeCreated;

    public UnAckRequest(){
      timeCreated = System.currentTimeMillis();
    }


    @Override
    public int compareTo(UnAckRequest other) {
      return  Long.valueOf(timeCreated).compareTo(other.timeCreated);
    }

    public long getAge(){
      return System.currentTimeMillis() - timeCreated;
    }

  }

  




  public class WFQScheduler {
    Map<String, LinkedList<PendingReceive>> receiveQueues;
    private int numReceives;
    private FCFSManager manager;
    private boolean prioritizeEarlierReplicas;
    public boolean fsRunning = true;
    
    
    public WFQScheduler(FCFSManager manager){
      receiveQueues = new ConcurrentHashMap<String, LinkedList<PendingReceive>>();
      numReceives = 0;
      this.manager = manager;
      this.prioritizeEarlierReplicas = manager.prioritizeEarlierReplicas;
    }

  
    
    public long getMaxAge(){
      long maxAge = 0;
      long temp;
      //For each priority class
      for (Entry<String, LinkedList<PendingReceive>> entry : receiveQueues.entrySet()) {
        //If the class is active
        if(!entry.getValue().isEmpty()){
          //get the age of the last block in the queue
          temp = entry.getValue().getFirst().getAge();
          //Update max age if necessary
          if(temp > maxAge)
          {
            maxAge = temp;
          }
        }
      }

      return maxAge;
    }

    public int getSize(){
      return numReceives;
    }

    public boolean isEmpty(){
      return (receiveQueues.isEmpty());
    }

    synchronized public void remove(long blockID, String flowName){
      LOG.info("AMDG : RFPR : REQUEST : " + blockID);
      LOG.info("AMDG : RFPR : CALLED : SIZE : " + receives.getSize());

      boolean isFound = false;

      PendingReceive current ;

      if(flowName.contentEquals("default")){
        for(Entry<String, LinkedList<PendingReceive>> entry : receiveQueues.entrySet()){
          //if queue for the priority level is not empty
          if(!entry.getValue().isEmpty()){
            Iterator<PendingReceive> it = entry.getValue().iterator();
            while(it.hasNext() && !isFound){
              current = it.next();
              if(current.blockID.equals(Long.valueOf(blockID).toString())){
                isFound = true;
                LOG.info("AMDG : RFPR : FOUND : " + current.blockID);
                it.remove();
                numReceives--;
                try{
                  manager.notifyUpStream(current.sourceIP, "remove," + current.blockID);
                }catch(IOException e){
                  LOG.warn("YOHWE : " + e.getMessage());
                }
              }
            }
          }
        }
        if(receiveQueues.get(flowName).isEmpty()){
          receiveQueues.remove(flowName);
        }
      }else{
        LinkedList<PendingReceive> currentQueue = receiveQueues.get(flowName);

        if(currentQueue != null){
          Iterator<PendingReceive> it = currentQueue.iterator();
          while(it.hasNext() && !isFound){
            current = it.next();
            //LOG.info("AMDG : RFPR : " + current.blockID);
            if(current.blockID.equals(Long.valueOf(blockID).toString())){
              isFound = true;
              LOG.info("AMDG : RFPR : FOUND : " + current.blockID);
              it.remove();
              numReceives--;
              if(receiveQueues.get(flowName).isEmpty()){
                receiveQueues.remove(flowName);
              }
              try{
                manager.notifyUpStream(current.sourceIP, "remove," + current.blockID);
              }catch(IOException e){
                LOG.warn("YOHWE : " + e.getMessage());
              }
            }
          }
        }
      }
    }

    private long getVirtualTime(){
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      long temp;
      for(Entry<String, LinkedList<PendingReceive>> entry : receiveQueues.entrySet()){
        //if queue for the priority level is not empty
        if(!entry.getValue().isEmpty()){
          try{
            temp = entry.getValue().getFirst().startTime;
            if(temp > max){
              max = temp;
            }
            if(temp < min){
              min = temp;
            }

          }catch(Exception e){
            LOG.warn("GVT : " + e.getMessage());
          }

        }
      }

      return ((min+max)/2);
    }

    synchronized public void addReceive(PendingReceive rec){
      LinkedList<PendingReceive> currentQueue = receiveQueues.get(rec.flowName);
      if(currentQueue == null){
        currentQueue = new LinkedList<PendingReceive>();
        receiveQueues.put(rec.flowName, currentQueue);
      }

      //set start time
      if(!currentQueue.isEmpty()){
        rec.startTime = Math.max(getVirtualTime(), currentQueue.getLast().finishTime); 
      }else{
        rec.startTime = getVirtualTime();
      }
      //set finish time
      //no longer scaling the 
      
      float queuingPriority = rec.replicationPriority;
      rec.finishTime = rec.startTime + (long)(rec.blockSize/queuingPriority);

      //finally insert rec into queue
      putInQueue(currentQueue,rec);
      numReceives++;
    }

    synchronized public PendingReceive getReceive(){
      String bestFlow = "";
      long bestTime = Long.MAX_VALUE;
      long temp;

      for(Entry<String, LinkedList<PendingReceive>> entry : receiveQueues.entrySet()){
        //LOG.info("RQUEUE," + entry.getKey() + "," + entry.getValue().size());
        //if queue for the priority level is not empty
        if(!entry.getValue().isEmpty()){
          temp = entry.getValue().getFirst().startTime;
          if(temp < bestTime){
            bestTime = temp;
            bestFlow = entry.getKey();
          }
        }
      }

      if(bestFlow.length() < 1){
        return null;
      }

      if(!prioritizeEarlierReplicas){
        numReceives--;
        PendingReceive result = receiveQueues.get(bestFlow).removeFirst();
        if(receiveQueues.get(bestFlow).isEmpty()){
          receiveQueues.remove(bestFlow);
        }
        return result;
      }else{
        numReceives--;
        PendingReceive result =getEarliestReplica(receiveQueues.get(bestFlow));
        if(receiveQueues.get(bestFlow).isEmpty()){
          receiveQueues.remove(bestFlow);
        }
        return result;
      }



    }

    public int getNumQueues(){
      return receiveQueues.size();
    }

    synchronized void putInQueue(LinkedList<PendingReceive> currentQueue, PendingReceive temp){
      currentQueue.add(temp);
    }

    synchronized public PendingReceive  getEarliestReplica(LinkedList<PendingReceive> currentQueue){
      int minIndex = 0;
      int minPosition = currentQueue.get(0).pipelinePosition;
      int tempPosition;
      LOG.info("DZUDEPOSBEF:" + minPosition);
      if(minPosition > 1){
        for(int i = 0; i < currentQueue.size() ;i++){
          tempPosition = currentQueue.get(i).pipelinePosition;
          if(tempPosition < minPosition){
            minPosition = tempPosition;
            minIndex=i;
          }
        }
        
      }
      LOG.info("DZUDEPOSAFT:" + minPosition);
      if(minIndex == 0){
        return currentQueue.removeFirst();
      }else{
        for(int i=0; i < minIndex; i++){
          currentQueue.get(i).startTime= currentQueue.get(i+1).startTime;
          currentQueue.get(i).finishTime= currentQueue.get(i+1).finishTime;
        }
        return currentQueue.remove(minIndex);
      }


    }
  }
  
}
