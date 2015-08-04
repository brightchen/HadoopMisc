package cg.hadoop.write;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriteToFileTask extends WriteTask
{
  private static final Logger LOG = LoggerFactory.getLogger(WriteToFileTask.class);
  
  final private Configuration conf = new Configuration();
  private FSDataOutputStream fsOutStream;

  public WriteToFileTask( String fileName )
  {
    Path filePath = new Path(fileName);
    try {
      FileSystem fs = FileSystem.get(conf);
      fsOutStream = fs.create(filePath);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  protected void write( byte[] data, int start, int length ) throws IOException
  {
    fsOutStream.write(data, start, length);
  }
  
}
