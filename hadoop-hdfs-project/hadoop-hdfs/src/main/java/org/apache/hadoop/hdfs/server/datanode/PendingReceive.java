package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.hdfs.server.datanode.FCFSManager.PFPUtils;


public class PendingReceive implements Comparable<PendingReceive>{
  public String blockID;
  public String sourceIP;
  public long blockSize;
  public float flowPriority;
  public String flow;
  private final long timeCreated;
  public String position;
  public float positionPriority;

  public PendingReceive(String message,float pPriorityRatio){
    String[] parts = PFPUtils.split(message);
    sourceIP = parts[0];
    blockID = parts[1];
    blockSize = Long.valueOf(parts[2]).longValue();
    flowPriority = Float.valueOf(parts[3]).floatValue();
    flow = parts[4];
    position = parts[5];
    timeCreated = System.currentTimeMillis();
    positionPriority = (float)Math.pow((double)pPriorityRatio, (double)Float.valueOf(position).floatValue());
  }


  @Override
  public int compareTo(PendingReceive other) {
    return  Long.valueOf(timeCreated).compareTo(other.timeCreated);
  }

  public long getAge(){
    return System.currentTimeMillis() - timeCreated;
  }

}

