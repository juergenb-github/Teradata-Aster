package utils;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import com.asterdata.ncluster.sqlmr.data.RowIterator;

/**
 * @author jb185040
 * Wrap input rows of type character varying into one combined single Reader.
 * This is useful to read a full row that was divided into chunks.
 */
public class PartitionReader extends Reader {
	private RowIterator inputIterator;
	private int columnIdx;
	private StringReader stringReader = null; 
	
	/**
	 * @param inputIterator of one combination of rows.
	 * @param columnIdx of the column containing the byte data
	 */
	public PartitionReader(RowIterator inputIterator, int columnIdx) {
		this.inputIterator = inputIterator;
		this.columnIdx = columnIdx;
		try {
			advanceToNextRow();
		} catch (IOException e) {} // Cannot happen at first call
	}

	@Override
	public int read() throws IOException {
		if(stringReader == null) return -1; // Reader already closed
		int ret = 0;
		
		for(ret = stringReader.read(); ret == -1; ret = stringReader.read()) {
			if(!advanceToNextRow()) return -1; // Behind last row
		}
		return ret;
	}

	@Override
	public int read(char[] b, int off, int len) throws IOException {
		if(stringReader == null) return -1; // Reader already closed
		int ret = 0;
		
		for(ret = stringReader.read(b, off, len); ret == -1; ret = stringReader.read(b, off, len)) {
			if(!advanceToNextRow()) return -1; // Behind last row
		}
		return ret;
	}

	@Override
	public int read(char[] b) throws IOException {
		return read(b, 0, b.length); // Just make sure it works as expected and documented
	}

	@Override
	public void close() throws IOException {
		if(stringReader != null) stringReader.close();
		stringReader = null;
	}

	/**
	 * @return the inputIterator to get the current row and handle all other columns.
	 */
	public RowIterator getInputIterator() {return inputIterator;}
	
	// Advance to next row and set input stream
	private boolean advanceToNextRow() throws IOException {
		// ignore rows with null value in char column
		while(inputIterator.advanceToNextRow()) {
			if(inputIterator.isNullAt(columnIdx)) continue;
			
			if(stringReader != null) stringReader.close();
			stringReader = new StringReader(inputIterator.getStringAt(columnIdx));
			return true;
		}
		stringReader = null;
		return false;
	}
}
