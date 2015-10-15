package org.apache.hadoop.hdfs.server.datanode;

import java.util.LinkedList;

public class LeafWFQ extends WeightedFairQueue {
  LinkedList<PendingReceive> receives = new LinkedList<PendingReceive>();

  LeafWFQ(){
   
  }
  
  void addReceive(PendingReceive receive) {
    receives.offerFirst(receive);
  }

  @Override
  PendingReceive getReceive() {
    return receives.pollFirst();
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
    return 0;
  }

  @Override
  int getSize() {
    return receives.size();
  }

  @Override
  long getStartTime() {
    if(!receives.isEmpty()){
      return receives.getFirst().jStart;
    }
    return 0;
  }

  @Override
  long getFinishTime() {
    if(!receives.isEmpty()){
      return receives.getLast().jEnd;
    }
    return 0;
  }
  
}
