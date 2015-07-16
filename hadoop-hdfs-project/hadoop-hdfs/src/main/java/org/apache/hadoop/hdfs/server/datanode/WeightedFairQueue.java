package org.apache.hadoop.hdfs.server.datanode;

import java.util.LinkedList;


public abstract class WeightedFairQueue {
    LinkedList<BTime> times ;
    WeightedFairQueue parent = null;
    
    abstract void addReceive(PendingReceive receive);
    abstract PendingReceive getReceive();
    abstract boolean isEmpty();
    abstract long getVirtualTime();
    
    abstract PendingReceive remove(String blockID, String flow, String position);
    abstract int getSize();
    abstract long getStartTime();
    abstract long getFinishTime();
}

