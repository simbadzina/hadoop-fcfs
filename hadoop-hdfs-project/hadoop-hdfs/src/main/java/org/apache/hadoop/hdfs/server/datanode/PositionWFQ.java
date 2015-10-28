package org.apache.hadoop.hdfs.server.datanode;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PositionWFQ extends WeightedFairQueue {
  Map<String, JobWFQ> queues;
  
  
  PositionWFQ(){
    times  = new LinkedList<BTime>();
    queues = new ConcurrentHashMap<String, JobWFQ>();
  }
  
  @Override
  synchronized void addReceive(PendingReceive receive) {
    String positionToUse = receive.position;
    JobWFQ curr = queues.get(positionToUse);
    if(curr == null){
      curr = new JobWFQ();
      queues.put(positionToUse, curr);
    }
      
    long startTime = this.getVirtualTime();
    if(!curr.isEmpty()){
      startTime = Math.max(startTime, curr.getFinishTime());
   }
    
   receive.pStart = startTime;
   receive.pEnd = startTime + (long)(receive.blockSize/receive.positionPriority);
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
    String positionToUse = position;
    JobWFQ curr = queues.get(positionToUse);
    PendingReceive removed = null;
    if(curr != null){
      removed = curr.remove(blockID,flow,positionToUse);
      if(removed !=null){
        if(curr.isEmpty()){
          queues.remove(positionToUse);
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
