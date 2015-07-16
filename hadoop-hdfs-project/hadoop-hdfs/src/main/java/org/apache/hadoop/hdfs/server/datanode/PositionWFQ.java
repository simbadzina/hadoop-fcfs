package org.apache.hadoop.hdfs.server.datanode;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PositionWFQ extends WeightedFairQueue {
  Map<String, JobWFQ> queues;
  
  
  PositionWFQ(){
    time = 0;
    queues = new ConcurrentHashMap<String, JobWFQ>();
  }
  
  @Override
  synchronized void addReceive(PendingReceive receive) {
    JobWFQ curr = queues.get(receive.position);
    if(curr == null){
      curr = new JobWFQ();
      queues.put(receive.position, curr);
    }
      
    
    if(curr.isEmpty()){
      long startTime = this.getVirtualTime();
      curr.setTime(startTime);
    }
    curr.addReceive(receive);

  }

  @Override
  synchronized PendingReceive getReceive() {
    String bestFlow = "";
    long bestTime = Long.MAX_VALUE;
    long temp;
    JobWFQ bestQueue;

    for(Entry<String, JobWFQ> entry : queues.entrySet()){
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
    long blockFinishTime =bestQueue.getTime() + (long)(result.blockSize/result.positionPriority);
    long startTime = Math.max(blockFinishTime, this.getVirtualTime());
    result.setTimeStamp(bestQueue.getTime());
    
    if(bestQueue.isEmpty()){
       queues.remove(bestFlow);
    }else{
      bestQueue.setTime(startTime);
    }   
    
    return result;
    
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

}
