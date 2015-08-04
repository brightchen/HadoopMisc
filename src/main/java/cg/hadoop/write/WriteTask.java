package cg.hadoop.write;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WriteTask implements Runnable{
  private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);
  
  final private Lock lock = new ReentrantLock();
  final private Condition writing  = lock.newCondition(); 
  
  private byte[] data;
  private int start;
  private int length;
  
  @Override
  public void run() {
    try {
      lock.lock();
      writing.await();
      lock.unlock();
      logger.debug("writing data: size: ", data.length);
      write(data, start, length);
    } catch (InterruptedException e) {
      e.printStackTrace();
      lock.unlock();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  protected abstract void write( byte[] data, int start, int length ) throws Exception;
  
  public void setData( byte[] data, int start, int length )
  {
    lock.lock();
    this.data = data;
    this.start = start;
    this.length = length;
    writing.signal();
    lock.unlock();
  }
}
