package utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

/**
 * @author jb185040
 * Separate one input stream or one input reader into chunks and deliver them to a caller.
 */
public class Chunkifier {
	/**
	 * @author jb185040
	 * Implement this to react on chunks read from an input stream as bytes without encoding.
	 */
	public interface ByteChunkListener {
		/**
		 * Called whenever a chunk of data was read from the input. Usually the rows are emitted from here.
		 * @param chunk number of a chunk within the input.
		 * @param buf Actual buffer of data. Cannot be larger then given chunk size.
		 * @param off Always 0.
		 * @param len Actual length of valid data within buffer.
		 */
		public void newChunk(int chunk, byte[] buf, int off, int len);
	}
	
	/**
	 * @author jb185040
	 * Implement this to react on chunks read from an input reader as characters with encoding.
	 */
	public interface CharChunkListener {
		/**
		 * Called whenever a chunk of data was read from the input. Usually the rows are emitted from here.
		 * @param chunk number of a chunk within the input.
		 * @param buf Actual buffer of character. Cannot be larger then given chunk size.
		 * @param off Always 0.
		 * @param len Actual length of valid data within buffer.
		 */
		public void newChunk(int chunk, char[] buf, int off, int len);
	}
	
	/**
	 * Read input stream chunk by chunk with the given size and send to listener.
	 * @param inputStream
	 * @param chunkSize
	 * @param byteChunkListener
	 * @throws IOException
	 */
	public static void read(InputStream inputStream, int chunkSize, ByteChunkListener byteChunkListener) throws IOException {
		byte[] buf = new byte[chunkSize];
		int chunk = 0;
		int off = 0, len = chunkSize;
		
		for(int eof = inputStream.read(buf, off, len); eof != -1; eof = inputStream.read(buf, off, len)) {
			off += eof;
			len -= eof;
			
			// If buffer full send out and reset buffer
			if(len <= 0) { 
				byteChunkListener.newChunk(chunk++, buf, 0, chunkSize);
				off = 0;
				len = chunkSize;
			}
		}
		
		// Send out last buffer
		if(chunkSize > len) byteChunkListener.newChunk(chunk++, buf, 0, chunkSize-len);
	}
	
	/**
	 * Read input stream chunk by chunk with the given size and send to listener.
	 * @param inputStream
	 * @param chunkSize
	 * @param byteChunkListener
	 * @throws IOException
	 */
	public static void read(Reader reader, int chunkSize, CharChunkListener charChunkListener) throws IOException {
		char[] buf = new char[chunkSize];
		int chunk = 0;
		int off = 0, len = chunkSize;
		
		for(int eof = reader.read(buf, off, len); eof != -1; eof = reader.read(buf, off, len)) {
			off += eof;
			len -= eof;
			
			// If buffer full send out and reset buffer
			if(len <= 0) { 
				charChunkListener.newChunk(chunk++, buf, 0, chunkSize);
				off = 0;
				len = chunkSize;
			}
		}
		
		// Send out last buffer
		if(chunkSize > len) charChunkListener.newChunk(chunk++, buf, 0, chunkSize-len);
	}
	
	// Do not allow to instantiate this class
	private Chunkifier() {}
}
