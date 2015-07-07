package org.apache.hadoop.hdfs.server.datanode;

import java.util.LinkedList;

public class LeafWFQ extends WeightedFairQueue {
  LinkedList<PendingReceive> receives = new LinkedList<PendingReceive>();

  LeafWFQ(){
    time = 0;
  }
  
  void addReceive(PendingReceive receive) {
    receives.push(receive);
  }

  @Override
  PendingReceive getReceive() {
    return receives.pop();
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
          return removed;
        }    
      }
     return null;
  }

  @Override
  long getVirtualTime() {
    return time;
  }

  @Override
  int getSize() {
    return receives.size();
  }
  
}
