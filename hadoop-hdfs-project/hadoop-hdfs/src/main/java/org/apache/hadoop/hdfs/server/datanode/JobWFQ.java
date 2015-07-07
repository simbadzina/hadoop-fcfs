package org.apache.hadoop.hdfs.server.datanode;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class JobWFQ extends WeightedFairQueue {
Map<String, LeafWFQ> queues;
  
  
  JobWFQ(){
    time = 0;
    queues = new ConcurrentHashMap<String, LeafWFQ>();
  }
  
  @Override
  void addReceive(PendingReceive receive) {
    LeafWFQ curr = queues.get(receive.flow);
    if(curr == null){
      curr = new LeafWFQ();
      queues.put(receive.flow, curr);
    }
      
    
    if(curr.isEmpty()){
      long finishTime = this.getVirtualTime() + (long)(receive.blockSize/receive.flowPriority);
      curr.setTime(finishTime);
    }
    curr.addReceive(receive);

  }

  @Override
  PendingReceive getReceive() {
    String bestFlow = "";
    long bestTime = Long.MAX_VALUE;
    long temp;
    LeafWFQ bestQueue;

    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      if(!entry.getValue().isEmpty()){
        temp = entry.getValue().getTime();
        if(temp < bestTime){
          bestTime = temp;
          bestFlow = entry.getKey();
        }
      }
    }

    if(bestFlow.length() < 1){
      return null;
    }else{
      bestQueue = queues.get(bestFlow);
    }

    PendingReceive result = bestQueue.getReceive();
    
    if(bestQueue.isEmpty()){
       queues.remove(bestFlow);
    }else{
      long startTime = Math.max(bestQueue.getTime(), this.getVirtualTime());
      long finishTime = startTime + (long)(result.blockSize/result.flowPriority);
      bestQueue.setTime(finishTime);
    }   
    return result;
    
  }

  @Override
  boolean isEmpty() {
    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      if(!entry.getValue().isEmpty()){
        return false;
      }
    }
    return true;
  }

  @Override
  long getVirtualTime() {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long temp;
    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      //if queue for the priority level is not empty
      if(!entry.getValue().isEmpty()){
          temp = entry.getValue().getTime();
          if(temp > max){
            max = temp;
          }
          if(temp < min){
            min = temp;
          }
      }
    }
    return ((min+max)/2);
  }

  @Override
  PendingReceive remove(String blockID, String flow, String position) {
    LeafWFQ curr = queues.get(flow);
    PendingReceive removed = null;
    if(curr != null){
      removed = curr.remove(blockID,flow,position);
      if(removed !=null){
        if(curr.isEmpty()){
          queues.remove(flow);
        }  
      }
    }  
    return removed;
  }

  @Override
  int getSize() {
    int size = 0;
    for(Entry<String, LeafWFQ> entry : queues.entrySet()){
      size += entry.getValue().getSize();
    }
    return size;
  }
}
