package sqlmrFunctions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;

import utils.Accumulator;
import utils.ErrorHandler;
import utils.PartitionInputStream;
import utils.PartitionReader;
import utils.UrlDecoderService;
import utils.UrlDecoderService.UrlDecoderCallback;
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
 * Returns a String decoded from the application/x-www-form-urlencoded MIME format.
 * 
 * Syntax:
 * 	SELECT * FROM UrlDecoder(
 * 		ON { table_name | view_name | (query) }
 * 		[PARTITION BY partition_column1, ... ORDER BY chunk]
 * 		value('column_name')
 * 		[encode(character encoding)]
 * 		[accumulate('passed_thru_column_name'[, ...])]
 * 		[unzip(character encoding)]
 * 		[stopOnError('true'|'false')]
 * 		[log(infoN)]
 * 
 * Parameter:
 * value	mandatory	Name of the column in the ON Phrase that contains encoded text. Column must be of type character varying or bytea,
 * 					depending on the unzip parameter.
 * accumulate	optional	Copy columns of input to each output row.
 * encode	optional	The name of a supported character encoding. The World Wide Web Consortium Recommendation states that UTF-8 should be used.
 * 						Not doing so may introduce incompatibilites.
 * unzip	optional	If set, the input is uncompressed before it is passed to the splitting routine.
 * 						The input must be of type bytea and is encoded by the given value. It is recommended in the current Aster
 * 						environment to encode with "UTF-8". If this parameter is not set, the function expects, that the input is
 * 						a plain text document. In this case the input must be of type character varying.
 * stopOnError	optional	Possible values are 'true' and 'false'. If set to 'true' the function stops when it occurs an error while
 * 							reading the partitions. 'false' indicates, that the function continues, ignoring the partition and continuing
 * 							with the next one. Default is 'false'.
 * 							The exception along with information about the partition is written to the log and returned to the user (stopOnError=true).
 * log optional	number of rows between information logged. If set, every number of rows or partitions, information is logged at
 * 							level INFO about timings and progress.	
 * 
 * Output:
 * The result consists of the uncompressed and encoded content and optionally meta data stored in each ZIP entry if parameter "unzip" was set.
 *
 * text	character varying	the decoded text.
 * 
 * Additional output is written if "unzip" is set. See SQL-MR function "unzip" for a description of the additional fields.
 * 
 * Error handling:
 * - Whenever a Java exception is thrown such as ZipException or an IOException, the partition is aborted and no more rows are returned.
 * - The exception and the stack is printed in the Aster log file. Processing continues with next partition.
 *  
 * DONE: Implement
 * TODO: Test cases for plain files and chunks
 * TODO: Documentation with Syntax, parameter explanation, output explanation, error messages, examples of call, examples how to work with result
 * TODO: Tell CoE about this function
 */
public final class UrlDecode implements RowFunction, PartitionFunction, Drainable {
	// These member variables will be populated with the values of the argument clauses passed to the SQL-MR function.
	private int textArgumentIdx = 0;
	private String encode = "UTF-8";
	private UrlDecoderService urlDecoderService = null;
	private Accumulator accumulator = null;
	private CharsetDecoder charsetDecoder = null; // if null, UNZIP was not set. Input is plain text.
	private ErrorHandler errorHandler = null;

	/* 
	 * The constructor establishes the RuntimeContract between
	 * the SQL-MR function and Aster Database. During query planning,
	 * the function will constructed on a single node. During
	 * query execution, it will be constructed and run on one or more nodes.
	 */
	public UrlDecode(RuntimeContract contract) {
		// Read argument clauses into appropriate member variables.
		textArgumentIdx = contract.getInputInfo().getColumnIndex(contract.useArgumentClause("value").getSingleValue());
		encode = Utils.getSingleStringFromParameter(contract, "encode", encode);
		accumulator = new Accumulator(contract);
		charsetDecoder = Utils.getEncoding(contract, "unzip");
		errorHandler = new ErrorHandler(
				this.getClass().getName(),
				Utils.getSingleBooleanFromParameter(contract, "stoponerror", false),
				Utils.getSingleIntFromParameter(contract, "log", Integer.MAX_VALUE));
		
		// Initialize the decoder
		urlDecoderService = new UrlDecoderService(encode);

		// Verify that the function accepts the given input schema depending on UNZIP is set or not.
		if(charsetDecoder != null && !SqlType.getType("bytea").equals(contract.getInputInfo().getColumnType(textArgumentIdx))) {
			throw new IllegalUsageException(
					"When 'zipped' is set, type (bytea) expected for zipped column \"" +
					contract.useArgumentClause("text").getSingleValue() + "\"");
		}
		else if(charsetDecoder == null && !SqlType.getType("character varying").equals(contract.getInputInfo().getColumnType(textArgumentIdx))) {
			throw new IllegalUsageException(
					"When 'zipped' not set, type (character varying) expected for text column \"" +
					contract.useArgumentClause("text").getSingleValue() + "\"");
		}

		// Construct the output schema
		List<ColumnDefinition> outputColumns = new ArrayList<ColumnDefinition>();
		accumulator.constructOutputSchema(contract, outputColumns);
		outputColumns.add(new ColumnDefinition("value", SqlType.getType("character varying")));

		if(charsetDecoder != null) {
			outputColumns.add(new ColumnDefinition("file", SqlType.getType("character varying")));
			outputColumns.add(new ColumnDefinition("time", SqlType.getType("timestamp with time zone")));
			outputColumns.add(new ColumnDefinition("size", SqlType.getType("integer")));
			outputColumns.add(new ColumnDefinition("compressedSize", SqlType.getType("integer")));
			outputColumns.add(new ColumnDefinition("isDirectory", SqlType.getType("character(1)")));
			outputColumns.add(new ColumnDefinition("method", SqlType.getType("integer")));
			outputColumns.add(new ColumnDefinition("crc", SqlType.getType("integer")));
			outputColumns.add(new ColumnDefinition("comment", SqlType.getType("character varying")));
		}
		contract.setOutputInfo(new OutputInfo(outputColumns));

		// Complete the contract
		contract.complete();
	}

	/* (non-Javadoc)
	 * @see com.asterdata.ncluster.sqlmr.RowFunction#operateOnSomeRows(com.asterdata.ncluster.sqlmr.data.RowIterator, com.asterdata.ncluster.sqlmr.data.RowEmitter)
	 * 
	 * Operate of rows of input. SQL-MR function was called as a map function without partitions.
	 * Each row is expected to be one complete Document, either compressed or plain.
	 */
	public void operateOnSomeRows(RowIterator inputIterator, RowEmitter outputEmitter) {
		while (inputIterator.advanceToNextRow()) {
			errorHandler.enterOperateOnRow(inputIterator, outputEmitter);
			if(inputIterator.isNullAt(textArgumentIdx)) {
				errorHandler.skipRow();
				continue;
			}

			if(charsetDecoder == null) // is input compressed?
				urlDecoder(urlDecoderService, new StringReader(inputIterator.getStringAt(textArgumentIdx)),
						null, accumulator, inputIterator, outputEmitter);
			else
				zippedUrlDecoder(new ByteArrayInputStream(inputIterator.getBytesAt(textArgumentIdx)),
						urlDecoderService, accumulator, charsetDecoder, inputIterator, outputEmitter);
			errorHandler.exitOperateOnRow();
		}
	}

	/* (non-Javadoc)
	 * @see com.asterdata.ncluster.sqlmr.PartitionFunction#operateOnPartition(com.asterdata.ncluster.sqlmr.data.PartitionDefinition, com.asterdata.ncluster.sqlmr.data.RowIterator, com.asterdata.ncluster.sqlmr.data.RowEmitter)
	 * 
	 * Operate of rows of input. SQL-MR function was called as a reduce function with partitions.
	 * Each partition is expected to be one complete Document, either compressed or plain.
	 * Rows within the partition are chunks (parts, pieces) of the document in correct order.
	 */
	public void operateOnPartition(PartitionDefinition definition, RowIterator inputIterator, RowEmitter outputEmitter) {
		errorHandler.enterOperateOnPartition(definition, inputIterator, outputEmitter);
		if(charsetDecoder == null) // is input compressed?
			urlDecoder(urlDecoderService, new PartitionReader(inputIterator, textArgumentIdx),
					null, accumulator, inputIterator, outputEmitter);
		else
			zippedUrlDecoder(new PartitionInputStream(inputIterator, textArgumentIdx),
					urlDecoderService, accumulator, charsetDecoder, inputIterator, outputEmitter);
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
	
	/**
	 * Split one document, not compressed and complete. Emit the row to Aster and handle skipping if detected.
	 * @param textSplitterService containing processing information.
	 * @param reader to read the text from.
	 * @param zipEntry might be null. If not null, contains the meta data of the ZIP entry to be emitted with each row.
	 * @param accumulator
	 * @param inputIterator
	 * @param outputEmitter
	 */
	private void urlDecoder(UrlDecoderService urlDecoderService,
			Reader reader, final ZipEntry zipEntry, final Accumulator accumulator,
			final RowIterator inputIterator, final RowEmitter outputEmitter) {
		
		try {
			urlDecoderService.urlDecode(reader, new UrlDecoderCallback() {
				// Construct row on each callback and emit the row
				public void newRow(String value) {
					
					// accumulate
					accumulator.emit(inputIterator, outputEmitter);

					// text part
					outputEmitter.addString(value);
					
					// ZIP part
					if(zipEntry != null) {
						Utils.emitNullableString(outputEmitter, zipEntry.getName());
						Utils.emitNullableTimestamp(outputEmitter, zipEntry.getTime(), -1L);
						Utils.emitNullableInteger(outputEmitter, zipEntry.getSize(), -1L);
						Utils.emitNullableInteger(outputEmitter, zipEntry.getCompressedSize(), -1L);
						outputEmitter.addString(zipEntry.isDirectory()? "T":"F");
						outputEmitter.addInt(zipEntry.getMethod());
						Utils.emitNullableInteger(outputEmitter, zipEntry.getCrc(), -1L);
						Utils.emitNullableString(outputEmitter, zipEntry.getComment());
					}
					outputEmitter.emitRow();
				}
			});
		} catch (IOException e) {
			errorHandler.catchException(e);
			return; // End this row and go to next if stopOnError is set to false (otherwise exception is thrown)
		}
	}
	
	/**
	 * Split one document, compressed and complete. Emit the row to Aster and handle skipping if detected.
	 * @param inputStream to read the compressed content from.
	 * @param textSplitterService containing processing information.
	 * @param accumulator
	 * @param charsetDecoder
	 * @param inputIterator
	 * @param outputEmitter
	 */
	private void zippedUrlDecoder(
			InputStream inputStream, final UrlDecoderService urlDecoderService,
			final Accumulator accumulator, final CharsetDecoder charsetDecoder,
			final RowIterator inputIterator, final RowEmitter outputEmitter) {
		
		try {
			// The unzip of the file
			Utils.unzip(inputStream, new UnzipCallback() {
				public void newZipEntry(final ZipEntry zipEntry, InputStream inputStream) {
					urlDecoder(urlDecoderService, new InputStreamReader(inputStream, charsetDecoder),
							zipEntry, accumulator, inputIterator, outputEmitter);
				}
			});
			inputStream.close();
		}
		catch(IOException e) {
			errorHandler.catchException(e);
			return; // End this row and go to next if stopOnError is set to false (otherwise exception is thrown)
		}
	}
}
