package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.datanode.FCFSManager.PFPUtils;
import org.apache.hadoop.hdfs.server.datanode.FCFSManager.StoManager;
import org.apache.hadoop.hdfs.server.datanode.FCFSManager.UnAckRequest;


public class PendingReceive implements Comparable<PendingReceive>,Runnable{
  public String blockID;
  public String sourceIP;
  public long blockSize;
  public float flowPriority;
  public String flow;
  private final long timeCreated;
  public String position;
  public float positionPriority;
  private long timestamp;
  private final StoManager manager;
  
  public long pStart;
  public long pEnd;
  
  public long jStart;
  public long jEnd;
  
  

  public PendingReceive(StoManager _manager,String message,float pPriority){
    String[] parts = PFPUtils.split(message);
    sourceIP = parts[0];
    blockID = parts[1];
    blockSize = Long.valueOf(parts[2]).longValue();
    flowPriority = Float.valueOf(parts[3]).floatValue();
    flow = parts[4];
    position = parts[5];
    timeCreated = System.currentTimeMillis();
    
    positionPriority = pPriority;
    manager = _manager;
    
  }

  public void setTimeStamp(long time){
    timestamp = time;
  }
  
  @Override
  public int compareTo(PendingReceive other) {
    return  Long.valueOf(timeCreated).compareTo(other.timeCreated);
  }

  public long getAge(){
    return System.currentTimeMillis() - timeCreated;
  }
  
  public String toString(){
    String res ;
    res = "PRECEIVE," + sourceIP + ","
        + blockID + "," 
        + blockSize + ","
        + flow + ","
        + flowPriority + ","
        + position + ","
        + positionPriority + ","
        + timestamp;
    return res;
  }

  @Override
  public void run() {
    // TODO Auto-generated method stub
    try{
      manager.manager.notifyUpStream(sourceIP, blockID);
    }catch(IOException e){
      
    }
  }

}

