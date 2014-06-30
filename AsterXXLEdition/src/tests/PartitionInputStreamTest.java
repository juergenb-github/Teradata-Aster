package tests;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.TestCase;
import utils.PartitionInputStream;

import com.asterdata.ncluster.sqlmr.data.RowIterator;
import com.asterdata.ncluster.sqlmr.data.SqlType;
import com.asterdata.ncluster.sqlmr.data.ValueHolder;
import com.asterdata.ncluster.sqlmr.data.types.Date;
import com.asterdata.ncluster.sqlmr.data.types.Time;
import com.asterdata.ncluster.sqlmr.data.types.Timestamp;
import com.asterdata.ncluster.util.ByteAppendable;
import com.asterdata.ncluster.util.ImmutableList;

public class PartitionInputStreamTest extends TestCase {
	private PartitionInputStream partitionInputStream = null;
	private MockupRowIterator mockupRowIterator = new MockupRowIterator();

	public void setUp() throws Exception {
		partitionInputStream = new PartitionInputStream(mockupRowIterator, 0);
	}

	public final void testPartitionReader() throws IOException {
		byte[] buf = new byte[5];
		byte[] bufResult = new byte[30];
		int off = 0;
		
		for(int r = partitionInputStream.read(buf); r != -1; r = partitionInputStream.read(buf)) {
			System.out.println(r + ", " + Arrays.toString(buf));
			System.arraycopy(buf, 0, bufResult, off, r);
			off += r;
		}
		System.out.println(Arrays.toString(bufResult));
		assertTrue(
				"Buffer with counting bytes from 1 to 20 expected. Filled with zeros",
				Arrays.equals(bufResult, new byte[] {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,0,0,0,0,0,0,0,0,0,0}));
	}

	private class MockupRowIterator implements RowIterator {
		private int currentRow = -1;
		private byte[][] testdata = new byte[][] {
				{1,2,3,4,5,6,7},
				{8,9,10},
				{},
				{11,12,13,14,15,16,17,18,19,20}
			};
		
		public byte[] getBytesAt(int columnIndex) {return testdata[currentRow];}

		public void getBytesAt(int columnIndex, ByteAppendable appendable) {}

		public int getColumnCount() {return 0;}

		public ImmutableList<SqlType> getColumnTypes() {return null;}

		public Date getDateAt(int columnIndex) {return null;}

		public double getDoubleAt(int columnIndex) {return 0;}

		public float getFloatAt(int columnIndex) {return 0;}

		public int getIntAt(int columnIndex) {return 0;}

		public long getLongAt(int columnIndex) {return 0;}

		public short getShortAt(int columnIndex) {return 0;}

		public String getStringAt(int columnIndex) {return null;}

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
