package sqlmrFunctions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;

import utils.Accumulator;
import utils.Chunkifier;
import utils.Chunkifier.ByteChunkListener;
import utils.Chunkifier.CharChunkListener;
import utils.ErrorHandler;
import utils.PartitionInputStream;
import utils.Utils;
import utils.Utils.UnzipCallback;

import com.asterdata.ncluster.sqlmr.Drainable;
import com.asterdata.ncluster.sqlmr.IllegalUsageException;
import com.asterdata.ncluster.sqlmr.OutputInfo;
import com.asterdata.ncluster.sqlmr.PartitionFunction;
import com.asterdata.ncluster.sqlmr.RowFunction;
import com.asterdata.ncluster.sqlmr.RuntimeContract;
import com.asterdata.ncluster.sqlmr.data.ColumnDefinition;
import com.asterdata.ncluster.sqlmr.data.PartitionDefinition;
import com.asterdata.ncluster.sqlmr.data.RowEmitter;
import com.asterdata.ncluster.sqlmr.data.RowIterator;
import com.asterdata.ncluster.sqlmr.data.SqlType;

/**
 * @author Juergen Boiselle
 *
 * Uncompresses a compressed binary input. The input usually comes from "ncluster_url_loader" or any other loader, that has done a binary load of a
 * compressed file.
 * 
 * Syntax:
 * 	SELECT * FROM UNZIP(
 * 		ON { table_name | view_name | (query) }
 * 		[PARTITION BY partition_column1, ... ORDER BY chunk]
 * 		ZIP('zipped_column_name')
 * 		[accumulate('passed_thru_column_name'[, ...])]
 * 		[encode(character encoding)]
 * 		[chunk(chunk size)]
 * 		[stopOnError('true'|'false')]
 * 		[log(infoN)]
 * 
 * Parameter:
 * ZIP	mandatory	Name of the column in the ON Phrase that contains the ZIP data in binary format. Column must be of type bytea.
 * accumulate	optional	Copy columns of input to each output row.
 * encode	optional	If set UNZIP returns its content as character varying encoded by the given value.
 * 						If not set the content is returned as binary value. It is recommended in the current Aster environment to encode with "UTF-8".
 * 						Consider smaller chunks if encoding to UTF-16 or other multi-byte character sets.
 * chunk	optional	Size of one chunk of resulting data. Default is 16MB.
 * stopOnError	optional	Possible values are 'true' and 'false'. If set to 'true' the function stops when it occurs an error while reading the partitions.
 * 							'false' indicates, that the function continues, ignoring the partition and continuing with the next one. Default is 'false'.
 * 							The exception along with information about the partition is written to the log and returned to the user (stopOnError=true).
 * log optional	number of rows between information logged	If set every number of rows or partitions, information is logged at level INFO about timings and progress.	
 * 
 * Output:
 * The result consists of the uncompressed content and meta data stored in each ZIP entry. If the compressed input is a plain file, then one
 * row, containing the uncompressed content, is returned. If the input is a directory, then a row for each ZIP entry is returned. If the content is larger
 * then the size defined for one chunk, a number of rows are returned, each containing 16MB or less.
 *
 * name	character varying	name of the entry.
 * time	timestamp with time zone	modification time of the entry
 * size	integer	uncompressed size of the entry data
 * compressedSize	integer	size of the compressed entry data
 * isDirectory	boolean	true if this is a directory entry
 * method	integer	compression method of the entry
 * crc	integer	the CRC-32 checksum of the uncompressed entry data
 * comment	character varying	the comment string for the entry, or null if none
 * chunk	integer	whenever the content is divided into chunks this is the ordering number.
 * content	bytea|character varying	the uncompressed content
 *
 * Error handling:
 * - Whenever a Java exception is thrown such as ZipException or an IOException, the partition is ignored and no rows are returned.
 * - The exception and the stack is printed in the Aster log file. Processing continues with next partition.
 *  
 * DONE: Implement
 * DONE: Test cases for files and directories with and without errors
 * TODO: Test Cases for integration test
 * TODO: Documentation with Syntax, parameter explanation, output explanation, error messages, examples of call, examples how to work with result
 * TODO: Tell CoE about this function
 */
public final class unzip implements RowFunction, PartitionFunction, Drainable {
	// These member variables will be populated with the values of the argument clauses passed to the SQL-MR function.
	private int zippedArgumentIdx = 0;
	private CharsetDecoder charsetDecoder = null;
	private Accumulator accumulator = null;
    private int chunkSize = Utils.CHUNK_SIZE;
	private ErrorHandler errorHandler = null;

	/* 
	 * The constructor establishes the RuntimeContract between
	 * the SQL-MR function and Aster Database. During query planning,
	 * the function will constructed on a single node. During
	 * query execution, it will be constructed and run on one or more nodes.
	 */
	public unzip(RuntimeContract contract) {
		// Read argument clauses into appropriate member variables.
		zippedArgumentIdx = contract.getInputInfo().getColumnIndex(contract.useArgumentClause("zip").getSingleValue());
		charsetDecoder = Utils.getEncoding(contract, "encode");
		accumulator = new Accumulator(contract);
		chunkSize = Utils.getSingleIntFromParameter(contract, "chunk", Utils.CHUNK_SIZE);
		errorHandler = new ErrorHandler(
				this.getClass().getName(),
				Utils.getSingleBooleanFromParameter(contract, "stoponerror", false),
				Utils.getSingleIntFromParameter(contract, "log", Integer.MAX_VALUE));

		// Verify that the function accepts the given input schema.
		if(!SqlType.getType("bytea").equals(contract.getInputInfo().getColumnType(zippedArgumentIdx)))
			throw new IllegalUsageException("Type (bytea) expected for zipped column \"" + contract.useArgumentClause("zip").getSingleValue() + "\"");

		// Construct the output schema
		List<ColumnDefinition> outputColumns = new ArrayList<ColumnDefinition>();

		accumulator.constructOutputSchema(contract, outputColumns);
		outputColumns.add(new ColumnDefinition("name", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("time", SqlType.getType("timestamp with time zone")));
		outputColumns.add(new ColumnDefinition("size", SqlType.getType("integer")));
		outputColumns.add(new ColumnDefinition("compressedSize", SqlType.getType("integer")));
		outputColumns.add(new ColumnDefinition("isDirectory", SqlType.getType("character(1)")));
		outputColumns.add(new ColumnDefinition("method", SqlType.getType("integer")));
		outputColumns.add(new ColumnDefinition("crc", SqlType.getType("integer")));
		outputColumns.add(new ColumnDefinition("comment", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("chunk", SqlType.getType("integer")));
		outputColumns.add(new ColumnDefinition("content", SqlType.getType((charsetDecoder != null)? "character varying":"bytea")));
		contract.setOutputInfo(new OutputInfo(outputColumns));

		// Complete the contract
		contract.complete();
	}

	/* (non-Javadoc)
	 * @see com.asterdata.ncluster.sqlmr.PartitionFunction#operateOnPartition(com.asterdata.ncluster.sqlmr.data.PartitionDefinition, com.asterdata.ncluster.sqlmr.data.RowIterator, com.asterdata.ncluster.sqlmr.data.RowEmitter)
	 * 
	 * Operate of rows of input. SQL-MR function was called as a reduce function with partitions.
	 * Each partition is expected to be one complete, compressed file.
	 * Rows within the partition are chunks (parts, pieces) of the file in correct order.
	 */
	public void operateOnSomeRows(final RowIterator inputIterator, final RowEmitter outputEmitter) {
		while (inputIterator.advanceToNextRow()) {
			errorHandler.enterOperateOnRow(inputIterator, outputEmitter);
			if(inputIterator.isNullAt(zippedArgumentIdx)) {
				errorHandler.skipRow();
				continue;
			}

			unzipSingle(new ByteArrayInputStream(inputIterator.getBytesAt(zippedArgumentIdx)), inputIterator, outputEmitter);
			errorHandler.exitOperateOnRow();
		}
	}
	
	/* (non-Javadoc)
	 * Read one partition, uncompress it and write it to the output. Each partition contains one compressed input, that might have been divided into chunks.
	 * The output as uncompressed data is written again in chunks.
	 * 
	 * @see com.asterdata.ncluster.sqlmr.PartitionFunction#operateOnPartition(com.asterdata.ncluster.sqlmr.data.PartitionDefinition, com.asterdata.ncluster.sqlmr.data.RowIterator, com.asterdata.ncluster.sqlmr.data.RowEmitter)
	 */
	public void operateOnPartition(PartitionDefinition definition, RowIterator inputIterator, RowEmitter outputEmitter) {
		errorHandler.enterOperateOnPartition(definition, inputIterator, outputEmitter);
		unzipSingle(new PartitionInputStream(inputIterator, zippedArgumentIdx), inputIterator, outputEmitter);
		errorHandler.exitOperateOnPartition();
	}

	/* (non-Javadoc)
	 * @see com.asterdata.ncluster.sqlmr.Drainable#drainOutputRows(com.asterdata.ncluster.sqlmr.data.RowEmitter)
	 * 
	 * Send last logging information
	 */
	public void drainOutputRows(RowEmitter outputEmitter) {
		errorHandler.drainOutputRows();
	}

	// inflate one file and emit rows.
	private void unzipSingle(final InputStream inputStream, final RowIterator inputIterator, final RowEmitter outputEmitter) {
		try {
			Utils.unzip(inputStream, new UnzipCallback() {
				public void newZipEntry(final ZipEntry zipEntry, InputStream inputStream) {
					if(charsetDecoder == null) { // Keep binary
						try {
							Chunkifier.read(inputStream, chunkSize, new ByteChunkListener() {
								public void newChunk(int chunk, byte[] buf, int off, int len) {
									emitHead(inputIterator, outputEmitter, zipEntry, chunk); // Head
									outputEmitter.addBytes(buf, off, len); // Content
									outputEmitter.emitRow(); // Emit to row
								}
							});
						} catch (IOException e) {
							errorHandler.catchException(e);
							return; // End this row and go to next if stopOnError is set to false (otherwise exception is thrown)
						}
					}
					else { // encode
						try {
							Chunkifier.read(new InputStreamReader(inputStream, charsetDecoder), chunkSize, new CharChunkListener() {
								public void newChunk(int chunk, char[] buf, int off, int len) {
									emitHead(inputIterator, outputEmitter, zipEntry, chunk); // Head
									outputEmitter.addString(new String(buf, off, len)); // Content
									outputEmitter.emitRow(); // Emit to row
								}
							});
						} catch (IOException e) {
							errorHandler.catchException(e);
							return; // End this row and go to next if stopOnError is set to false (otherwise exception is thrown)
						}
					}
				}
			});
		} catch (IOException e) {
			errorHandler.catchException(e);
			return; // End this row and go to next if stopOnError is set to false (otherwise exception is thrown)
		}
	}

	// Emit all but content and chunk to the row. 
	private void emitHead(RowIterator inputIterator, RowEmitter outputEmitter, ZipEntry zipEntry, int chunk) {
		// accumulate
		accumulator.emit(inputIterator, outputEmitter);

		// Meta data
		Utils.emitNullableString(outputEmitter, zipEntry.getName());
		Utils.emitNullableTimestamp(outputEmitter, zipEntry.getTime(), -1L);
		Utils.emitNullableInteger(outputEmitter, zipEntry.getSize(), -1L);
		Utils.emitNullableInteger(outputEmitter, zipEntry.getCompressedSize(), -1L);
		Utils.emitNullableString(outputEmitter, zipEntry.isDirectory()? "T":"F");
		Utils.emitNullableInteger(outputEmitter, zipEntry.getMethod(), -1L);
		Utils.emitNullableInteger(outputEmitter, zipEntry.getCrc(), -1L);
		Utils.emitNullableString(outputEmitter, zipEntry.getComment());
		outputEmitter.addInt(chunk);
	}
}
