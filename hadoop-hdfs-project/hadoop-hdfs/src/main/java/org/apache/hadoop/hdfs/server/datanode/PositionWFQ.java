package org.apache.hadoop.hdfs.server.datanode;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.mortbay.log.Log;

public class PositionWFQ extends WeightedFairQueue {
  Map<String, JobWFQ> queues;
  
  
  PositionWFQ(){
    times  = new LinkedList<BTime>();
    queues = new ConcurrentHashMap<String, JobWFQ>();
  }
  
  @Override
  synchronized void addReceive(PendingReceive receive) {
    JobWFQ curr = queues.get(receive.position);
    if(curr == null){
      curr = new JobWFQ();
      queues.put(receive.position, curr);
    }
      
    long startTime = this.getVirtualTime();
    if(!curr.isEmpty()){
      startTime = Math.max(startTime, curr.getFinishTime());
   }
    
   receive.pStart = startTime;
   receive.pEnd = startTime + (long)(receive.blockSize/receive.positionPriority);
   curr.addReceive(receive);

  }


  synchronized PendingReceive getReceive(Map<String,Integer> buffersCount, int maxReceives) {
    String bestFlow = "";
    long bestTime = Long.MAX_VALUE;
    long temp;
    JobWFQ bestQueue;
    Integer count;
    for(Entry<String, JobWFQ> entry : queues.entrySet()){
      count = buffersCount.get(entry.getKey());
      if(count != null){
        if(count >= maxReceives){
          Log.info("DIAG, position:" + entry.getKey() + " count= " + count);
          continue;
        }
      }
      if(!entry.getValue().isEmpty()){
        temp = entry.getValue().getStartTime();
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
    result.setTimeStamp(bestTime);
    
    if(bestQueue.isEmpty()){
       queues.remove(bestFlow);
    }
    return result;
    
  }
  
  @Override
  synchronized PendingReceive getReceive() {
    return null;
  }

  @Override
  boolean isEmpty() {
    for(Entry<String, JobWFQ> entry : queues.entrySet()){
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
    for(Entry<String, JobWFQ> entry : queues.entrySet()){
      //if queue for the priority level is not empty
      if(!entry.getValue().isEmpty()){
          temp = entry.getValue().getStartTime();
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
  synchronized PendingReceive remove(String blockID, String flow, String position) {
    JobWFQ curr = queues.get(position);
    PendingReceive removed = null;
    if(curr != null){
      removed = curr.remove(blockID,flow,position);
      if(removed !=null){
        if(curr.isEmpty()){
          queues.remove(position);
        }  
      }
    }  
    return removed;
  }

  @Override
  int getSize() {
    int size = 0;
    for(Entry<String, JobWFQ> entry : queues.entrySet()){
      size += entry.getValue().getSize();
    }
    return size;
  }

  @Override
  long getStartTime() {
    return 0;
  }

  @Override
  long getFinishTime() {
    return 0;
  }

}
