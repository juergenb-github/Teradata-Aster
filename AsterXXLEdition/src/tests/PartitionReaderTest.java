package tests;

import java.io.IOException;

import junit.framework.TestCase;
import utils.PartitionReader;

import com.asterdata.ncluster.sqlmr.data.RowIterator;
import com.asterdata.ncluster.sqlmr.data.SqlType;
import com.asterdata.ncluster.sqlmr.data.ValueHolder;
import com.asterdata.ncluster.sqlmr.data.types.Date;
import com.asterdata.ncluster.sqlmr.data.types.Time;
import com.asterdata.ncluster.sqlmr.data.types.Timestamp;
import com.asterdata.ncluster.util.ByteAppendable;
import com.asterdata.ncluster.util.ImmutableList;

public class PartitionReaderTest extends TestCase {
	private PartitionReader partitionReader = null;
	private MockupRowIterator mockupRowIterator = new MockupRowIterator();

	public void setUp() throws Exception {
		partitionReader = new PartitionReader(mockupRowIterator, 0);
	}

	public final void testPartitionReader() throws IOException {
		char[] buf = new char[1024];
		int off = 0;
		
		for(int r = partitionReader.read(buf, off, 5); r != -1; r = partitionReader.read(buf, off, 5)) {
			off += r;
			System.out.println(r + ", " + partitionReader.ready() + ", " + new String(buf));
		}
		assertEquals("resulting text not correct", "Hall�chenWeltwie geht es dir?", new String(buf).trim());
	}

	private class MockupRowIterator implements RowIterator {
		private int currentRow = -1;
		private String[] testdata = new String[] {
				"Hall�chen",
				"Welt",
				"",
				"wie geht es dir?"
			};
		
		public byte[] getBytesAt(int columnIndex) {return null;}

		public void getBytesAt(int columnIndex, ByteAppendable appendable) {}

		public int getColumnCount() {return 0;}

		public ImmutableList<SqlType> getColumnTypes() {return null;}

		public Date getDateAt(int columnIndex) {return null;}

		public double getDoubleAt(int columnIndex) {return 0;}

		public float getFloatAt(int columnIndex) {return 0;}

		public int getIntAt(int columnIndex) {return 0;}

		public long getLongAt(int columnIndex) {return 0;}

		public short getShortAt(int columnIndex) {return 0;}

		public String getStringAt(int columnIndex) {return testdata[currentRow];}

		public void getStringAt(int columnIndex, Appendable appendable) {}

		public Time getTimeAt(int columnIndex) {return null;}

		public Timestamp getTimestampAt(int columnIndex) {return null;}

		public void getValueAt(int columnIndex, ValueHolder valueHolder) {}

		public boolean isNullAt(int columnIndex) {return false;}

		public boolean advanceToNextRow() {
			currentRow++;
			return currentRow < testdata.length;
		}
	}
}
