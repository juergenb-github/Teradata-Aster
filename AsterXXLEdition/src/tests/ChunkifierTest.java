package tests;

import java.io.IOException;
import java.io.StringReader;

import junit.framework.TestCase;
import utils.Chunkifier;
import utils.Chunkifier.CharChunkListener;

public class ChunkifierTest extends TestCase {

	public final void testPartitionReader() throws IOException {
		final StringBuilder result = new StringBuilder();
		
		// Empty input
		Chunkifier.read(new StringReader(""), 5, new CharChunkListener() {
			public void newChunk(int chunk, char[] buf, int off, int len) {
				System.out.println(chunk  + ", " + new String(buf, off, len) + ", " + off + ", " + len);
				result.append(new String(buf, off, len));
				assertTrue("chunk max 0", chunk <= 0);
			}
		});
		assertEquals("empty input", "", result.toString());
		result.setLength(0);
		
		// small input
		Chunkifier.read(new StringReader("123"), 5, new CharChunkListener() {
			public void newChunk(int chunk, char[] buf, int off, int len) {
				System.out.println(chunk  + ", " + new String(buf, off, len) + ", " + off + ", " + len);
				result.append(new String(buf, off, len));
				assertTrue("chunk max 0", chunk <= 0);
			}
		});
		assertEquals("empty input", "123", result.toString());
		result.setLength(0);
		
		// large input
		Chunkifier.read(new StringReader("123456789012345678901234"), 5, new CharChunkListener() {
			public void newChunk(int chunk, char[] buf, int off, int len) {
				System.out.println(chunk  + ", " + new String(buf, off, len) + ", " + off + ", " + len);
				result.append(new String(buf, off, len));
				assertTrue("chunk max 4", chunk <= 4);
			}
		});
		assertEquals("empty input", "123456789012345678901234", result.toString());
		result.setLength(0);
		
		// exact input
		Chunkifier.read(new StringReader("1234567890"), 5, new CharChunkListener() {
			public void newChunk(int chunk, char[] buf, int off, int len) {
				System.out.println(chunk  + ", " + new String(buf, off, len) + ", " + off + ", " + len);
				result.append(new String(buf, off, len));
				assertTrue("chunk max 1", chunk <= 1);
			}
		});
		assertEquals("empty input", "1234567890", result.toString());
	}
}
