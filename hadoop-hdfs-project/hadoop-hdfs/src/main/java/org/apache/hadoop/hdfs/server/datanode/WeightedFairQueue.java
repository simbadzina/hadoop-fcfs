package org.apache.hadoop.hdfs.server.datanode;

public abstract class WeightedFairQueue {
    long time;
    WeightedFairQueue parent = null;
    
    long getTime(){
      return time;
    }
    
    void resetTime(){
      time = 0;
    }
    
    void setTime(long newTime){
        time = newTime; 
    }
    
    abstract void addReceive(PendingReceive receive);
    abstract PendingReceive getReceive();
    abstract boolean isEmpty();
    abstract long getVirtualTime();
    
    abstract PendingReceive remove(String blockID, String flow, String position);
    abstract int getSize();
}

