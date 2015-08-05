package cg.hadoop.write;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class WriteTask implements Runnable {
  private static final Logger logger = LoggerFactory.getLogger(WriteTask.class);

  private final Lock dataLock = new ReentrantLock();
  private final Condition setDataCond = dataLock.newCondition();
  
  private final Lock closeLock = new ReentrantLock();
  private final Condition closed = closeLock.newCondition();
  
  private final int timeOut = 1000;
  
  private byte[] data;
  private int start;
  private int length;
  
  private AtomicBoolean cleanup = new AtomicBoolean(false);
  
  public void startWriteThread()
  {
    new Thread(this).start();
  }
  
  @Override
  public void run() {
    logger.debug("write thread started.");
    
    
    
   
    while(!cleanup.get())
    {
      boolean success = false;
      int wroteSize = 0;
      byte[] wroteData = null;
      
      dataLock.lock();
      
      try {
        while( !cleanup.get() && ( data==null || length <= 0 ) )
          setDataCond.await( timeOut, TimeUnit.MILLISECONDS );
        if( cleanup.get() )
          break;
        
        logger.debug("writting data. data: {}, size: {}", System.identityHashCode(data), length);
        write(data, start, length);
        success = true;
        wroteSize = length;
        wroteData = data;
        
        data = null;
        length = 0;
      } catch (InterruptedException e) {
        success = false;
        logger.warn(e.getMessage());
      } catch (Exception e) {
        success = false;
        logger.error(e.getMessage());
      } finally {
        dataLock.unlock();
        writeDone( wroteData, success, wroteSize );
      }
      
      logger.debug("wrote size: {}", wroteSize);
    }
    
    closeLock.lock();
    closed.signal();
    closeLock.unlock();
    
    logger.info("Done.");
  }

  protected abstract void write(byte[] data, int start, int length) throws Exception;
  
  protected void writeDone(byte[] data, boolean success, int wroteSize){};

  public void flush(){}
  
  public void setData(byte[] data, int start, int length) {
    if(cleanup.get())
      return;
    
    if (data == null || length <= 0)
    {
      throw new IllegalArgumentException( String.format( "Invalid data or length. data: %d, lenghth: %d", System.identityHashCode(data), length));
    }
    
    dataLock.lock();
    this.data = data;
    this.start = start;
    this.length = length;
    logger.debug("data ready for writting. data: {}", System.identityHashCode(data));
    setDataCond.signal();
    dataLock.unlock();
    
    return;
  }
  
  public void cleanup() {
    cleanup.set(true);

    closeLock.lock();

    try {
      closed.await( timeOut, TimeUnit.MILLISECONDS );
    } catch (InterruptedException e) {
    }

    closeLock.unlock();
  }
}
