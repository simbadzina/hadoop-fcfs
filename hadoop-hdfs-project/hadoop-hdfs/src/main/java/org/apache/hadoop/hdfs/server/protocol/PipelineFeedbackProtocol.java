package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.KerberosInfo;

/** An inter-datanode protocol for pipeline feedback
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY,
    clientPrincipal = DFSConfigKeys.DFS_DATANODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface PipelineFeedbackProtocol {
  public static final Log LOG = LogFactory.getLog(PipelineFeedbackProtocol.class);

  public static final long versionID = 6L;
  
  /**
   *  Get the status of a datanode 
   */
  String informUpStream(String message) throws IOException;
  
  /**
   * Inform another datanode of your status
   */
  
  String informDownStream(String message) throws IOException;
}
