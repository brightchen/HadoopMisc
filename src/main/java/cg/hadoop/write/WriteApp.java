package cg.hadoop.write;

public class WriteApp {
  public static final String filePath = "/tmp/WriteApp.out";
  public final static int count = 1000;
  
  public static void main(String[] argvs)
  {
    BlockWriter writer = new BlockWriter( 10240, new WriteToFileTask(filePath) );
    
    byte[] data = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890000000000".getBytes();
    for( int i=0; i<count; ++i )
    { 
      writer.write( String.format("%4d", i).getBytes() );
      writer.write( data );
    }
    writer.flush();
  }
}
