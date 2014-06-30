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

import org.xml.sax.SAXException;

import utils.Accumulator;
import utils.ErrorHandler;
import utils.PartitionInputStream;
import utils.PartitionReader;
import utils.Utils;
import utils.Utils.UnzipCallback;
import utils.XMLReaderService;

import com.asterdata.ncluster.sqlmr.ClientVisibleException;
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
 * Parses XML documents and filters using an include and exclude list. The data is optionally uncompressed and encoded with a given character set
 * before XML parsing starts.
 * All nodes are returned, when not filtered out. Filtering is configured using the include and exclude list, which contain nodes to switch
 * filtering on and off.
 * 
 * Syntax:
 * 	SELECT * FROM unzipXMLFilter(
 * 		ON { table_name | view_name | (query) }
 * 		[PARTITION BY partition_column1, ... ORDER BY chunk]
 * 		XML('column_name')
 * 		[INCLUDE('included_node_localname|included_regex'[, ...])]
 * 		[EXCLUDE('excluded_node_localname|excluded_regex'[, ...])]
 * 		[SKIPAFTER('localname|regex'[, ...])]
 * 		[LOCALNAMES('localname|regex'[, ...])]
 * 		[accumulate('passed_thru_column_name'[, ...])]
 * 		[unzip(character encoding)]
 * 		[stopOnError('true'|'false')]
 * 		[log(infoN)]
 * 
 * Parameter:
 * XML	mandatory	Name of the column in the ON Phrase that contains the XML document.
 * accumulate	optional	Copy columns of input to each output row.
 * unzip	optional	If set, the input is uncompressed before it is passed to the XML parser.
 * 						The input must be of type bytea and is encoded by the given value. It is recommended in the current Aster
 * 						environment to encode with "UTF-8". If this parameter is not set, the function expects, that the input is a XML document.
 * 						In this case the input must be of type character varying.
 * stopOnError	optional	Possible values are 'true' and 'false'. If set to 'true' the function stops when it occurs an error while reading the partitions.
 * 							'false' indicates, that the function continues, ignoring the partition and continuing with the next one. Default is 'false'.
 * 							The exception along with information about the partition is written to the log and returned to the user (stopOnError=true).
 * log optional	number of rows between information logged	If set every number of rows or partitions, information is logged at level INFO about timings and progress.	
 * SKIPAFTER	optional	An item of this list contains either the local name of a node element or a regular expression.
 * 							See INCLUDE/EXCLUDE for rules on how regular expressions and the local name are interpreted.
 * 							The XML parser skips the rest of an XML document, whenever it has found one of the node elements defined.
 * 							The node element itself and its sub nodes are included in the parse. For
 * 							example you can skip an XML document after reading the header tag with ('header').
 * LOCALNAMES	optional	An item of this list contains either the local name of a node element or a regular expression.
 * 							See INCLUDE/EXCLUDE for rules on how regular expressions and the local name are interpreted.
 * 							The XML returns only rows with of of the local names of this list. If the list is omitted all local names are returned.
 * 
 * INCLUDE/EXCLUDE	optional
 * An item of each list either contains the local name of a node element or a regular expression.
 * A "/" as first character of the item indicates, that the node item is identified by a regular expression.
 * The regular expression is checked against the full path of the element. The full path is built out of all local names, separated by "/".
 * For example in a tree like
 * 
 * 	A
 * 		A1
 * 		A2
 * 	B
 * 		B1
 * 			B1a
 * 
 * the full path for B1a is "/B/B1/B1a/" and the regular expression is checked against this full path.
 * 
 * Each one list or both lists can be empty or null, which leads to different behavior:
 * 1. If both lists are empty or null, all nodes are included. No filtering applies.
 * 2. If only the exclude list is empty or null and the include list contains items:
 * 		No nodes are excluded by default, but all nodes matching the included items, either by direct match with the local
 * 		name or by regular expression matching with the full path, and all nodes in the sub tree below are included.
 * 3. If only the include list is empty or null and the exclude list contains items:
 * 		All nodes are included by default, except all nodes matching the excluded items, either by direct match with the local
 * 		name or by regular expression matching with the full path, and all nodes in the sub tree below are also excluded. 
 * 4. Both lists contain items:
 * 		No nodes are excluded by default, but all nodes matching the included items and all nodes in the sub tree below are included.
 * 		Up to a point where a node matches one of the items in the excluded list. From this point on the node and the sub nodes are excluded.
 * 		If on of the sub nodes is in the included list, this nodes and its sub nodes are included again. This can be done down to as many levels
 * 		as necessary.
 * 
 * A word about performance:
 * Basically two rules apply considering performance on this function.
 * 1. Keep the include and exclude list as simple as possible and use direct comparison instead of regular expressions where possible.
 * 2. Accumulate as least columns as possible, especially do not accumulate the XML column itself, as it can be large which needs a lot
 * 		time to copy into the result set. Keep in mind that the result set has significantly more rows then the input.
 * 
 * Output:
 * The result consists of the uncompressed and parsed content and optionally meta data stored in each ZIP entry if paramter "unzip" was set to true.
 *
 * uri	character varying	is the URI of the element or attribute.
 * localname	character varying	is the local name of the element or attribute.
 * qname	character varying	is the qualified name of the element or attribute.
 * type	character(1)	indicates if the returned element is a XML tag �T� or an attribute �A�.
 * value	character varying	is the data contained in the element or attribute as CDATA.
 * includeparent	character varying	is the local name of the element that was in the include list.
 * 		It can be used to group elements together into one row using the PIVOT function.
 * includecount	integer	is the count of �includeparent� to separate parents with same name.
 * fullpath	character varying	is the full path that is matched against the regular expressions in include and exclude list.
 * id	integer	is a unique id within the XML document. Each XML document in each input row restarts the id.
 * first_sub_id	integer	is the id of the first tag or attribute that is below this node.
 * 		The column is useful to select parts in the XML. See examples below on how to use this column.
 * 
 * Additional output is written if "unzip" is set to true. See SQL-MR function "unzip" for a description of the additional fields.
 * 
 * Error handling:
 * - Whenever a Java exception is thrown such as ZipException, XMLParsing or an IOException, the partition is aborted and no more rows are returned.
 * - The exception and the stack is printed in the Aster log file. Processing continues with next partition.
 *  
 * DONE: Implement
 * DONE: Test cases for plain files and chunks with and without errors
 * DONE: Test Cases for integration test
 * DONE: Documentation with Syntax, parameter explanation, output explanation, error messages, examples of call, examples how to work with result
 * DONE: Tell CoE about this function
 * DONE: XMLStreamReader is even faster? No, needs more overhead in the self written programming part.
 */
public final class XMLFastFilter implements RowFunction, PartitionFunction, Drainable {
	// These member variables will be populated with the values of the argument clauses passed to the SQL-MR function.
	private int xmlArgumentIdx = 0;
	private XMLReaderService xmlReaderService = null;
	private Accumulator accumulator = null;
	private CharsetDecoder charsetDecoder = null; // if null, UNZIP was not set. Input is plain XML.
	private ErrorHandler errorHandler = null;

	/* 
	 * The constructor establishes the RuntimeContract between
	 * the SQL-MR function and Aster Database. During query planning,
	 * the function will constructed on a single node. During
	 * query execution, it will be constructed and run on one or more nodes.
	 */
	public XMLFastFilter(RuntimeContract contract) {
		// Read argument clauses into appropriate member variables.
		xmlArgumentIdx = contract.getInputInfo().getColumnIndex(contract.useArgumentClause("xml").getSingleValue());
		accumulator = new Accumulator(contract);
		charsetDecoder = Utils.getEncoding(contract, "unzip");
		errorHandler = new ErrorHandler(
				this.getClass().getName(),
				Utils.getSingleBooleanFromParameter(contract, "stoponerror", false),
				Utils.getSingleIntFromParameter(contract, "log", Integer.MAX_VALUE));
		
		// Initialize the XML parser
		try {
			xmlReaderService = new XMLReaderService(
					contract.hasArgumentClause("include")? contract.useArgumentClause("include").getValues():null,
					contract.hasArgumentClause("exclude")? contract.useArgumentClause("exclude").getValues():null,
					contract.hasArgumentClause("skipafter")? contract.useArgumentClause("skipafter").getValues():null,
					contract.hasArgumentClause("localnames")? contract.useArgumentClause("localnames").getValues():null);
		} catch (SAXException e) {
			throw new ClientVisibleException(e.getMessage());
		}

		// Verify that the function accepts the given input schema depending on UNZIP is set or not.
		if(charsetDecoder != null && !SqlType.getType("bytea").equals(contract.getInputInfo().getColumnType(xmlArgumentIdx))) {
			throw new IllegalUsageException(
					"When 'zipped' is set, type (bytea) expected for zipped column \"" +
					contract.useArgumentClause("xml").getSingleValue() + "\"");
		}
		else if(charsetDecoder == null && !SqlType.getType("character varying").equals(contract.getInputInfo().getColumnType(xmlArgumentIdx))) {
			throw new IllegalUsageException(
					"When 'zipped' not set, type (character varying) expected for xml column \"" +
					contract.useArgumentClause("xml").getSingleValue() + "\"");
		}

		// Construct the output schema
		List<ColumnDefinition> outputColumns = new ArrayList<ColumnDefinition>();
		accumulator.constructOutputSchema(contract, outputColumns);
		outputColumns.add(new ColumnDefinition("includeparent", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("includecount", SqlType.getType("integer")));
		outputColumns.add(new ColumnDefinition("fullpath", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("uri", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("localname", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("qname", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("type", SqlType.getType("character (1)")));
		outputColumns.add(new ColumnDefinition("value", SqlType.getType("character varying")));
		outputColumns.add(new ColumnDefinition("id", SqlType.getType("integer")));
		outputColumns.add(new ColumnDefinition("first_sub_id", SqlType.getType("integer")));

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
	 * Each row is expected to be one complete XML Document, either compressed or plain.
	 */
	public void operateOnSomeRows(RowIterator inputIterator, RowEmitter outputEmitter) {
		while (inputIterator.advanceToNextRow()) {
			errorHandler.enterOperateOnRow(inputIterator, outputEmitter);
			if(inputIterator.isNullAt(xmlArgumentIdx)) {
				errorHandler.skipRow();
				continue;
			}

			if(charsetDecoder == null) // is input compressed?
				xmlFilter(xmlReaderService, new StringReader(inputIterator.getStringAt(xmlArgumentIdx)),
						null, accumulator, inputIterator, outputEmitter);
			else
				zippedXmlFilter(new ByteArrayInputStream(inputIterator.getBytesAt(xmlArgumentIdx)),
						xmlReaderService, accumulator, charsetDecoder, inputIterator, outputEmitter);
			errorHandler.exitOperateOnRow();
		}
	}

	/* (non-Javadoc)
	 * @see com.asterdata.ncluster.sqlmr.PartitionFunction#operateOnPartition(com.asterdata.ncluster.sqlmr.data.PartitionDefinition, com.asterdata.ncluster.sqlmr.data.RowIterator, com.asterdata.ncluster.sqlmr.data.RowEmitter)
	 * 
	 * Operate of rows of input. SQL-MR function was called as a reduce function with partitions.
	 * Each partition is expected to be one complete XML Document, either compressed or plain.
	 * Rows within the partition are chunks (parts, pieces) of the document in correct order.
	 */
	public void operateOnPartition(PartitionDefinition definition, RowIterator inputIterator, RowEmitter outputEmitter) {
		errorHandler.enterOperateOnPartition(definition, inputIterator, outputEmitter);
		if(charsetDecoder == null) // is input compressed?
			xmlFilter(xmlReaderService, new PartitionReader(inputIterator, xmlArgumentIdx),
					null, accumulator, inputIterator, outputEmitter);
		else
			zippedXmlFilter(new PartitionInputStream(inputIterator, xmlArgumentIdx),
					xmlReaderService, accumulator, charsetDecoder, inputIterator, outputEmitter);
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
	 * Parse the XML of one XML document, not compressed and complete. Emit the row to Aster and handle skipping if detected.
	 * @param xmlReaderService containing processing information like include and exclude lists.
	 * @param reader to read the XML from.
	 * @param zipEntry might be null. If not null, contains the meta data of the ZIP entry to be emitted with each row.
	 * @param accumulator
	 * @param inputIterator
	 * @param outputEmitter
	 */
	private void xmlFilter(XMLReaderService xmlReaderService,
			Reader reader, final ZipEntry zipEntry, final Accumulator accumulator,
			final RowIterator inputIterator, final RowEmitter outputEmitter) {
		
		try {
			xmlReaderService.parse(reader, new XMLReaderService.XMLReaderCallback() {
				// Construct row on each callback and emit the row
				public void newRow(String includeParent, int includeCount, String fullPath,
						String uri, String localName, String qName, String type, String value, int id, int first_sub_id) {
					
					// accumulate
					accumulator.emit(inputIterator, outputEmitter);

					// XML part
					Utils.emitNullableString(outputEmitter, includeParent);
					outputEmitter.addInt(includeCount);
					outputEmitter.addString(fullPath);
					outputEmitter.addString(uri);
					outputEmitter.addString(localName);
					outputEmitter.addString(qName);
					outputEmitter.addString(type);
					outputEmitter.addString(value);
					outputEmitter.addInt(id);
					outputEmitter.addInt(first_sub_id);
					
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
		} catch (SAXException e) {
			errorHandler.catchException(e);
			return; // End this row and go to next if stopOnError is set to false (otherwise exception is thrown)
		} catch (IOException e) {
			errorHandler.catchException(e);
			return; // End this row and go to next if stopOnError is set to false (otherwise exception is thrown)
		}
	}
	
	/**
	 * Parse the XML of one XML document, not compressed and complete. Emit the row to Aster and handle skipping if detected.
	 * @param inputStream to read the compressed content from.
	 * @param xmlReaderService containing processing information like include and exclude lists.
	 * @param accumulator
	 * @param charsetDecoder
	 * @param inputIterator
	 * @param outputEmitter
	 */
	private void zippedXmlFilter(
			InputStream inputStream, final XMLReaderService xmlReaderService,
			final Accumulator accumulator, final CharsetDecoder charsetDecoder,
			final RowIterator inputIterator, final RowEmitter outputEmitter) {
		
		try {
			// The unzip of the file
			Utils.unzip(inputStream, new UnzipCallback() {
				public void newZipEntry(final ZipEntry zipEntry, InputStream inputStream) {
					xmlFilter(xmlReaderService, new InputStreamReader(inputStream, charsetDecoder), zipEntry, accumulator, inputIterator, outputEmitter);
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
