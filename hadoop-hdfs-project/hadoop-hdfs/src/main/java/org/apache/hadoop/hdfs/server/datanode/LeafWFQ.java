package org.apache.hadoop.hdfs.server.datanode;

import java.util.LinkedList;

public class LeafWFQ extends WeightedFairQueue {
  LinkedList<PendingReceive> receives = new LinkedList<PendingReceive>();
  LinkedList<BTime> times = new LinkedList<BTime>();
  
  LeafWFQ(){
   
  }
  
  void addReceive(PendingReceive receive) {
    times.addLast(new BTime(receive.pStart,receive.pEnd));
    receives.addLast(receive);
  }

  @Override
  PendingReceive getReceive() {
    times.pollFirst();
    
    //change the line below to change from queue to stack
    //first is queue
    return receives.pollLast();
  }

  @Override
  boolean isEmpty() {
    return receives.isEmpty();
  }
  

  PendingReceive remove(String blockID, String flow, String position) {
    PendingReceive removed; 
    for(int i = 0; i < receives.size();i++){
        removed = receives.get(i);
        if(removed.blockID.equals(blockID)){
          receives.remove(i);  
          times.pollFirst();
          return removed;
        }    
      }
     return null;
  }

  @Override
  long getVirtualTime() {
    return 0;
  }

  @Override
  int getSize() {
    return receives.size();
  }

  @Override
  long getStartTime() {
    if(!times.isEmpty()){
      return times.getFirst().start;
    }
    return 0;
  }

  @Override
  long getFinishTime() {
    if(!times.isEmpty()){
      return times.getLast().end;
    }
    return 0;
  }
  
}
