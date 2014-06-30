package utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.asterdata.ncluster.sqlmr.data.RowIterator;

/**
 * @author jb185040
 * Wrap input rows of type bytea into one combined single InputStream.
 * This is useful to read a full row that was divided into chunks.
 */
public class PartitionInputStream extends InputStream {
	private RowIterator inputIterator;
	private int columnIdx;
	private ByteArrayInputStream byteArrayInputStream = null; 
	
	/**
	 * @param inputIterator of one combination of rows.
	 * @param columnIdx of the column containing the byte data
	 * @throws IOException 
	 */
	public PartitionInputStream(RowIterator inputIterator, int columnIdx) {
		this.inputIterator = inputIterator;
		this.columnIdx = columnIdx;
		try {
			advanceToNextRow();
		} catch (IOException e) {} // Cannot happen at first call
	}

	@Override
	public int read() throws IOException {
		if(byteArrayInputStream == null) return -1; // Reader already closed
		int ret = 0;
		
		for(ret = byteArrayInputStream.read(); ret == -1; ret = byteArrayInputStream.read()) {
			if(!advanceToNextRow()) return -1; // Behind last row
		}
		return ret;
	}

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if(byteArrayInputStream == null) return -1; // Reader already closed
		int ret = 0;
		
		for(ret = byteArrayInputStream.read(b, off, len); ret == -1; ret = byteArrayInputStream.read(b, off, len)) {
			if(!advanceToNextRow()) return -1; // Behind last row
		}
		return ret;
	}

	@Override
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length); // Just make sure it works as expected and documented
	}

	/**
	 * @return the inputIterator to get the current row and handle all other columns.
	 */
	public RowIterator getInputIterator() {return inputIterator;}
	
	// Advance to next row and set input stream
	private boolean advanceToNextRow() throws IOException {
		// ignore rows with null value in byte column
		while(inputIterator.advanceToNextRow()) {
			if(inputIterator.isNullAt(columnIdx)) continue;
			
			if(byteArrayInputStream != null) byteArrayInputStream.close();
			byteArrayInputStream = new ByteArrayInputStream(inputIterator.getBytesAt(columnIdx));
			return true;
		}
		byteArrayInputStream = null;
		return false;
	}
}
