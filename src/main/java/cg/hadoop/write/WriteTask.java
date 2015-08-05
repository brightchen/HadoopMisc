package cg.hadoop.write;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WriteTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);

  final private Lock lock = new ReentrantLock();
  final private Condition writing = lock.newCondition();

  private byte[] data;
  private int start;
  private int length;

  @Override
  public void run() {
    int wroteSize = 0;
    boolean success = false;
   
    while(true)
    {
      lock.lock();
      
      try {
        while( data==null || length <= 0 )
          writing.await();
        logger.debug("writting data. data: {}", System.identityHashCode(data));
        write(data, start, length);
        success = true;
        wroteSize = length;
        data = null;
        length = 0;
      } catch (InterruptedException e) {
        success = false;
        logger.warn(e.getMessage());
      } catch (Exception e) {
        success = false;
        logger.error(e.getMessage());
      } finally {
        lock.unlock();
        writeDone( success );
      }
      
      logger.debug("wrote size: {}", wroteSize);
    }
  }

  protected abstract void write(byte[] data, int start, int length) throws Exception;

  protected abstract void writeDone(boolean success);

  public void setData(byte[] data, int start, int length) {
    if (data == null || length <= 0)
      return;

    lock.lock();
    this.data = data;
    this.start = start;
    this.length = length;
    logger.debug("data ready for writting. data: {}", System.identityHashCode(data));
    writing.signal();
    lock.unlock();
  }
}
