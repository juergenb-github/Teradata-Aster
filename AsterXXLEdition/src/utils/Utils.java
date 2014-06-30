package utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import java.util.Calendar;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import utils.Chunkifier.ByteChunkListener;

import com.asterdata.ncluster.sqlmr.ClientVisibleException;
import com.asterdata.ncluster.sqlmr.IllegalUsageException;
import com.asterdata.ncluster.sqlmr.RuntimeContract;
import com.asterdata.ncluster.sqlmr.data.RowEmitter;
import com.asterdata.ncluster.sqlmr.data.RowView;
import com.asterdata.ncluster.sqlmr.data.SqlType;
import com.asterdata.ncluster.sqlmr.data.ValueHolder;
import com.asterdata.ncluster.sqlmr.data.types.Timestamp;

/**
 * @author jb185040
 * Some utility methods, that seem to be useful.
 */
public class Utils {
	/**
	 * @author jb185040
	 * Implement this interface to get informed about entries found during an unzip process.
	 */
	public interface UnzipCallback {
		/**
		 * Indicate, that a new entry was found in the zipped input stream. The entry is closed by the caller after this method returns.
		 * @param zipEntry
		 * @param inputStream containing the content of the zipped entry.
		 */
		public void newZipEntry(ZipEntry zipEntry, InputStream inputStream);
	}
	
	/**
	 * Default size of a chunk in bytes.
	 */
	public static final int CHUNK_SIZE = 16*1024*1204;
	
	/**
	 * Return boolean from optional parameter with single value. Return default value if parameter was not set.
	 * @param contract
	 * @param parameter
	 * @param def
	 * @return true or false depending on content of parameter or, if not set by user, the default.
	 * @throws IllegalUsageException if parameter does not contain a valid value.
	 */
	static public boolean getSingleBooleanFromParameter(RuntimeContract contract, String parameter, boolean def) throws IllegalUsageException {
		// If not given, return default
		if(!contract.hasArgumentClause(parameter)) return def;
		
		// If given convert to boolean
		String value = contract.useArgumentClause(parameter).getSingleValue();
		if("true".equalsIgnoreCase(value))
			return true;
		else if("false".equalsIgnoreCase(value))
			return false;
		else
			throw new IllegalUsageException(parameter + ": unexpected value '" + value + "'. Expected either 'true' or 'false'");
	}
	
	/**
	 * Return integer from optional parameter with single value. Return default value if parameter was not set.
	 * @param contract
	 * @param parameter
	 * @param def
	 * @return a number depending on content of parameter or, if not set by user, the default.
	 * @throws IllegalUsageException if parameter does not contain a valid value.
	 */
	public static int getSingleIntFromParameter(RuntimeContract contract, String parameter, int def) throws IllegalUsageException {
		// If not given, return default
		if(!contract.hasArgumentClause(parameter)) return def;
		
		// If given convert to integer
		String value = contract.useArgumentClause(parameter).getSingleValue();
		try {
			return Integer.parseInt(value);
		} catch(NumberFormatException e) {
			throw new IllegalUsageException(parameter + ": unexpected value '" + value + "'. Expected an integer number");
		}
	}
	
	/**
	 * Return string from optional parameter with single value. Return default value if parameter was not set.
	 * @param contract
	 * @param parameter
	 * @param def
	 * @return String depending on content of parameter or, if not set by user, the default.
	 * @throws IllegalUsageException if parameter does not contain a valid value.
	 */
	public static String getSingleStringFromParameter(RuntimeContract contract, String parameter, String def) throws IllegalUsageException {
		// If not given, return default
		return contract.hasArgumentClause(parameter)? contract.useArgumentClause(parameter).getSingleValue():def;
	}

	/**
	 * Return Character set decoder for encoding with an InputStreamReader from optional parameter with single value.
	 * Return null if parameter was not set. Parameter is "encoding".
	 * @param contract
	 * @return null, if parameter is not set, else return a new character set decoder, that replaces characters not mappable or invalid. 
	 * @throws IllegalUsageException if parameter does not contain a valid character set name.
	 */
	public static CharsetDecoder getEncoding(RuntimeContract contract, String parameter) throws IllegalUsageException {
		if (contract.hasArgumentClause(parameter)) try {
			return getEncoding(contract.useArgumentClause(parameter).getSingleValue());
		}
		catch(IllegalArgumentException e) {
			throw new IllegalUsageException(e.getMessage());
		}
		return null;
	}

	/**
	 * Return Character set decoder for encoding with an InputStreamReader from encoding.
	 * @return a new character set decoder, that replaces characters not mappable or invalid. 
	 * @throws IllegalArgumentException if parameter does not contain a valid character set name.
	 */
	public static CharsetDecoder getEncoding(String encode) throws IllegalArgumentException {
		CharsetDecoder charsetDecoder = null;
		
		try {
			charsetDecoder = Charset.forName(encode).newDecoder();
			charsetDecoder.onMalformedInput(CodingErrorAction.REPLACE);
			charsetDecoder.onUnmappableCharacter(CodingErrorAction.REPLACE);
		}
		catch(IllegalArgumentException e) {
			// Add a hint to error message
			throw new IllegalArgumentException("Not a supported character set \"" + encode + "\". Try UTF-8. " + e.getMessage());
		}
		return charsetDecoder;
	}
	
	/**
	 * Emit an Integer that might be null
	 * @param outputEmitter
	 * @param value
	 * @param nullValue indicates which value represents the null and should be replaced by null in the database.
	 */
	static public void emitNullableInteger(RowEmitter outputEmitter, long value, long nullValue) {
		if(value != nullValue)
			outputEmitter.addLong(value);
		else
			outputEmitter.addNull();
	}
	
	/**
	 * Emit a String that might be null.
	 * @param outputEmitter
	 * @param value
	 */
	static public void emitNullableString(RowEmitter outputEmitter, String value) {
		if(value != null)
			outputEmitter.addString(value);
		else
			outputEmitter.addNull();
	}
	
	/**
	 * Emit a time stamp that might be null.
	 * @param outputEmitter
	 * @param value
	 * @param nullValue indicates which value represents the null and should be replaced by null in the database.
	 */
	static public void emitNullableTimestamp(RowEmitter outputEmitter, long value, long nullValue) {
		if(value != nullValue) {
			Calendar creationTimestamp = Calendar.getInstance();
			creationTimestamp.setTimeInMillis(value);
			outputEmitter.addTimestamp(new Timestamp(
					SqlType.getType("timestamp with time zone"),
					creationTimestamp.get(Calendar.YEAR),
					creationTimestamp.get(Calendar.MONTH)+1,
					creationTimestamp.get(Calendar.DAY_OF_MONTH),
					creationTimestamp.get(Calendar.HOUR_OF_DAY),
					creationTimestamp.get(Calendar.MINUTE),
					creationTimestamp.get(Calendar.SECOND),
					creationTimestamp.get(Calendar.MILLISECOND)*1000000));
		}
		else
			outputEmitter.addNull();
	}

	/**
	 * Print error message on standard error and throw Client visible exception if stopOnError is set to true. In
	 * addition to the error message print information of a row, usually the partition definition.
	 * @param exception
	 * @param stopOnError
	 * @param partition
	 * @param current row
	 */
	public static void catchException(Exception exception, boolean stopOnError, RowView partition, RowView current, String... extras) {
		exception.printStackTrace();
		
		if(stopOnError)
			throw new ClientVisibleException(
					exception.getMessage() + ", " +
					rowToString("partition", partition) + ", " +
					rowToString("current row", current) + ", " +
					"extra(\n\t" + Arrays.toString(extras) + "\n)");
		else {
			System.err.println(rowToString("partition", partition));
			System.err.println(rowToString("current row", current));
			System.err.println("extra(\n\t" + Arrays.toString(extras) + "\n)");
		}
	}

	/**
	 * Print the prefix, an opening bracket, new line, each column content in a line with leading tab and newline at the end and a closing bracket.
	 * @param prefix
	 * @param rowView
	 * @return row content information or an empty string, if rowView is null.
	 */
	public static String rowToString(String prefix, RowView rowView) {
		if(rowView == null) return "";
		StringBuilder sb = new StringBuilder();
		
		sb.append(prefix);
		sb.append("(\n");
		try {
			for(int i = 0; i < rowView.getColumnCount(); i++) {
				sb.append('\t');
	
					ValueHolder valueHolder = new ValueHolder(rowView.getColumnTypes().get(i));
					rowView.getValueAt(i, valueHolder);
					
					if(valueHolder.isNull())
						sb.append("(null)");
					else if(valueHolder.getValueType() == SqlType.getType("bytea"))
						sb.append("(bytea)");
					else if(valueHolder.getValueType() == SqlType.getType("character varying")) {
						String value = valueHolder.toString();
						if(value.length() > 70) value = value.substring(0, 67) + "...";
						sb.append(value);
					}
					else
						valueHolder.toString(sb);
				sb.append('\n');
			}
		}
		catch(RuntimeException e) {
			sb.append('\t');
			sb.append(e.getMessage());
			sb.append('\n');
		}
		sb.append(')');
		return sb.toString();
	}
	
	/**
	 * Read input as zipped file and return each entry found.
	 * @param inputStream
	 * @param unzipListener
	 * @throws IOException 
	 */
	public static void unzip(InputStream inputStream, UnzipCallback unzipListener) throws IOException {
		ZipInputStream zis = new ZipInputStream(inputStream);
		
		for(ZipEntry zipEntry = zis.getNextEntry(); zipEntry != null; zipEntry = zis.getNextEntry()) {
			unzipListener.newZipEntry(zipEntry, zis);
			zis.closeEntry();
		}
	}

	/**
	 * Compress input stream and provide result as chunks. Starts its own thread for piping the data.
	 * @param inputStream to read uncompressed data from.
	 * @param pipeSize of the internal buffer to decouple reading from writing.
	 * @throws IOException 
	 */
	public static void zip(final String name, final InputStream inputStream, final int pipeSize, ByteChunkListener byteChunkListener) throws IOException {
		// inputStream -> GZIPOutputStream -> Chunkifier -> chunks of Byte to listener
		final PipedInputStream readFromPipe = new PipedInputStream(pipeSize);
		final PipedOutputStream writeCompressedIntoPipe = new PipedOutputStream(readFromPipe); // Make sure it's connected now
		
		// Start thread that reads from input and pass it to compressor
		Thread thread = new Thread(new Runnable() {
			public void run() {
				byte[] buf = new byte[pipeSize];
				try {
					ZipOutputStream compressor = new ZipOutputStream(writeCompressedIntoPipe);
					compressor.putNextEntry(new ZipEntry(name));
					for(int len = inputStream.read(buf); len != -1; len = inputStream.read(buf)) compressor.write(buf, 0, len);
					compressor.closeEntry();
					compressor.finish();
					compressor.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		});
		thread.start();
		
		// Read on pipe end compressed data
		Chunkifier.read(readFromPipe, pipeSize, byteChunkListener);
		
		// Cleanup
		while(thread.isAlive()) try {
			thread.join();
		} catch (InterruptedException e) {}
		writeCompressedIntoPipe.close();
		readFromPipe.close();
	}
	
	// replace one invalid XML char. Tests are done in order of most occurrences.
	static public char replaceInvalid(char in) {
		if(0x20 <= in && in <= 0xd7ff) return in;
		if(in == 0x9 || in == 0xa || in == 0xd) return in;
		if(0xe000 <= in && in <= 0xfffd) return in;
		if(0x10000 <= in && in <= 0x10ffff) return in;
		return '?';
	}

	/**
	 * Find max long value in a list of longs
	 * @param values
	 * @return Maximal value out of list of values. Long.MIN_VALUE if the list is empty.
	 */
	static public long max(long... values) {
		long maxValue = Long.MIN_VALUE;
		
		for(long value : values)
			if(value > maxValue) maxValue = value;
		return maxValue;
	}
	
	// Do not allow to instantiate this class
	private Utils() {}
}
