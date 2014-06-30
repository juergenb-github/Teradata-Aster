package utils;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.asterdata.ncluster.sqlmr.ClientVisibleException;
import com.asterdata.ncluster.sqlmr.data.PartitionDefinition;
import com.asterdata.ncluster.sqlmr.data.RowEmitter;
import com.asterdata.ncluster.sqlmr.data.RowIterator;
import com.asterdata.ncluster.sqlmr.data.SqlType;

/**
 * @author jb185040
 * 
 * Handling of errors and logging events. This class handles exceptions thrown and events like row processing happen.
 * It calculates run times, logs with the appropriate level into log4j, throws Client visible exceptions as defined
 * by the parameters passed to the SQL-MR function.
 * All events and errors during processing are handled. Errors happening during construction time are likely to be detected
 * immediately and have no special methods here.
 * For performance reasons, this class is not thread safe. A separate instance should be used for each thread.
 * 
 * Events and log levels:
 * SEVERE:	An exception was thrown.
 * 			IF stopOnError is true, the current row or partition is skipped and processing stops with a Client visible exception.
 * 			IF stopOnError is false, the current row or partition is skipped and processing continues on next row or partition. 
 * WARNING:	A row or partition was skipped.
 * INFO:	All counters and timings are logged. This happens at least at the end of a SQL-MR statement.
 * 			Additionally a threshold can be set so that the INFO logging is written every N rows or
 * 			partitions during execution of the SQL-MR statement.
 * FINE:	When exiting a row or partition, all counters and timings are logged. This can produce
 * 			a large amount of logging information. Use with care.
 * 
 * For default levels and logging configuration, please check JRE_HOME/lib/logging.properties.
 * All parameters like name of the logger and level is written to standard error at the beginning.
 */
public class ErrorHandler {
	private Logger logger = null;
	private boolean stopOnError = false;
	private int infoN = 0;
	private Level configuredLimit = null;
	private PartitionDefinition partitionDefinition = null;
	private RowIterator inputIterator = null;
	private RowEmitter outputEmitter = null;
	private int cntRowsEntered = 0, cntRowsExited = 0, cntRowsSkipped = 0;
	private int cntPartitionsEntered = 0, cntPartitionsExited = 0, cntPartitionsSkipped = 0;
	private int cntExceptions = 0;
	private long elapsedMs = 0L, inRowMs = 0L, inPartitionMs = 0L;
	
	/**
	 * Create a new ErrorHandler.
	 * @param name of the logger, reflecting the logger configuration. Usually this.class.getName()
	 * @param stopOnError as set by a parameter.
	 * @param infoN print out additional INFO logs with progress information every n'th row and partition.
	 */
	public ErrorHandler(String name, boolean stopOnError, int infoN) {
		// Reread logging configuration 
		try {
			LogManager.getLogManager().readConfiguration();
		} catch (SecurityException e) {;
		} catch (IOException e) {;
		}
		
		logger = Logger.getLogger(name);
		this.stopOnError = stopOnError;
		this.infoN = infoN;
		isLogged(Level.ALL);
		System.err.println("Logging level is defined in \"" + System.getProperty("java.home") + "/lib/logging.properties\" as:");
		System.err.println(name + ".level = " + configuredLimit.getName());
		System.err.println("user.name: " + System.getProperty("user.name"));
		System.err.println("user.home: " + System.getProperty("user.home"));
		System.err.println("user.dir: " + System.getProperty("user.dir"));
		elapsedMs -= System.currentTimeMillis();
	}

	/**
	 * Call whenever starting to work on a single row, usually at the beginning of a loop thru rows in "operateOnPartition"
	 * 
	 * @param inputIterator
	 * @param outputEmitter
	 */
	public void enterOperateOnRow(RowIterator inputIterator, RowEmitter outputEmitter) {
		this.inputIterator = inputIterator;
		this.outputEmitter = outputEmitter;
		cntRowsEntered++;
		inRowMs -= System.currentTimeMillis();
	}
	
	/**
	 * Call whenever a row is skipped due to a quality check or the like.
	 * This automatically clears the output rows to prepare it for the next row.
	 * The skipped row is logged as warning.
	 */
	public void skipRow() {
		exitOperateOnRow();

		cntRowsSkipped++;
		logger.log(Level.WARNING, buildMessage(Level.WARNING, "row skipped"));
		if(outputEmitter != null) outputEmitter.clearRow();
	}
	
	/**
	 * Call whenever finished working on a single row, usually at the end of a loop thru rows in "operateoOnSomeRows".
	 */
	public void exitOperateOnRow() {
		if(cntRowsEntered == cntRowsExited) return; // Already exited in another way
		
		inRowMs += System.currentTimeMillis();
		cntRowsExited++;

		logger.log(Level.FINE, buildMessage(Level.FINE, "exit row"));
		if(cntRowsExited % infoN == 0) logger.log(Level.INFO, buildMessage(Level.INFO, "exit row"));
	}
	
	/**
	 * Call whenever starting to work on a partition, usually at the beginning of "operateOnPartition"
	 * 
	 * @param definition
	 * @param inputIterator
	 * @param outputEmitter
	 */
	public void enterOperateOnPartition(PartitionDefinition definition, RowIterator inputIterator, RowEmitter outputEmitter) {
		this.partitionDefinition = definition;
		this.inputIterator = inputIterator;
		this.outputEmitter = outputEmitter;
		cntPartitionsEntered++;
		inPartitionMs -= System.currentTimeMillis();
	}

	/**
	 * Call whenever a partition is skipped due to a quality check or the like.
	 * This automatically clears the output rows to prepare it for the next partition.
	 * The skipped partition is logged as warning.
	 */
	public void skipPartition() {
		exitOperateOnPartition();

		cntPartitionsSkipped++;
		logger.log(Level.WARNING, buildMessage(Level.WARNING, "partition skipped"));
		if(outputEmitter != null) outputEmitter.clearRow();
	}
	
	/**
	 * Call whenever finished working on a partition, usually at the end of "operateoOnPartition".
	 */
	public void exitOperateOnPartition() {
		if(cntPartitionsEntered == cntPartitionsExited) return; // Already exited in another way
		
		inPartitionMs += System.currentTimeMillis();
		cntPartitionsExited++;

		logger.log(Level.FINE, buildMessage(Level.FINE, "exit partition"));
		if(cntPartitionsExited % infoN == 0) logger.log(Level.INFO, buildMessage(Level.INFO, "exit partition"));
	}

	/**
	 * Call this method whenever a exception was thrown. It either throws a Client visible
	 * exception or logs the error and continues depending on stopOnError Parameter. It also
	 * skips the row and partition as appropriate.
	 *  
	 * @param exception
	 */
	public void catchException(Exception exception, String... extras) {
		exitOperateOnRow();
		exitOperateOnPartition();
		cntExceptions++;
		
		// Build log message
		String logMessage = buildMessage(Level.SEVERE, exception.getMessage(), extras);
		
		// React on exception
		logger.log(Level.SEVERE, logMessage, exception);
		if(stopOnError) throw new ClientVisibleException(logMessage);
		
		if(outputEmitter != null) outputEmitter.clearRow();
	}
	
	/**
	 * Call in the "drainOutputRows" method. Logs all final counters and timings.
	 */
	public void drainOutputRows() {
		elapsedMs += System.currentTimeMillis();
		logger.log(Level.INFO, buildMessage(Level.INFO, "drain"));
		// This would be a good place to close the logger, but currently such a statement does not exist.
	}
	
	// Build message to log. First msg string is written up front, rest after partition, current row, output position and counters+timings.
	private String buildMessage(Level level, String prefix, String... msgs) {
		if(!isLogged(level)) return "";
		
		StringBuilder log = new StringBuilder();
		if(prefix != null) {
			log.append(prefix);
			log.append(",\n");
		}
		if(cntPartitionsEntered > 0) {
			log.append(Utils.rowToString("partition", partitionDefinition));
			log.append(",\n");
		}
		if(cntRowsEntered > 0) {
			log.append(Utils.rowToString("current input row", inputIterator));
			log.append(",\n");
		}
		
		// Output
		if(cntRowsEntered > 0) {
			log.append("current output row(\n\tcolumns added: ");
			log.append(outputEmitter.getAddedColumnCount());
			log.append("\n\tcolumns expected: ");
			log.append(outputEmitter.getExpectedColumnCount());
			log.append("\n\ttypes(\n");
			for(SqlType sqlType : outputEmitter.getExpectedColumnTypes()) {
				log.append("\t\t");
				log.append(sqlType.getCanonicalName());
				log.append("\n");
			}
			log.append("\t)\n)\n");
		}
		
		// Timings
		log.append("timings(\n\telapsed [ms]: ");
		log.append(elapsedMs + (elapsedMs < 0L? System.currentTimeMillis():0L));
		log.append("\n\tin partition [ms]: ");
		log.append(inPartitionMs);
		log.append("\n\tin row [ms]: ");
		log.append(inRowMs);
		log.append("\n)\n");

		// Counters
		if(cntPartitionsEntered > 0) {
			log.append("counters(");
			log.append("\n\tpartitions(\n\t\tentered: ");
			log.append(cntPartitionsEntered);
			log.append("\n\t\texited: ");
			log.append(cntPartitionsExited);
			log.append("\n\t\tskipped: ");
			log.append(cntPartitionsSkipped);
			log.append("\n\t)");
		}
		if(cntRowsEntered > 0) {
			log.append("\n\trows(\n\t\tentered: ");
			log.append(cntRowsEntered);
			log.append("\n\t\texited: ");
			log.append(cntRowsExited);
			log.append("\n\t\tskipped: ");
			log.append(cntRowsSkipped);
			log.append("\n\t)\n\texceptions: ");
			log.append(cntExceptions);
			log.append("\n)\n");
		}
		
		// Extras
		if(msgs.length > 0) {
			log.append("extra(\n");
			for(String msg : msgs) {
				log.append('\t');
				log.append(msg);
				log.append('\n');
			}
			log.append(")\n");
		}
		return log.toString();
	}
	
	// Check, if level is logged within current logger configuration
	private boolean isLogged(Level toPublish) {
		// Get the effective level
		for(Logger parent = logger; configuredLimit == null && parent != null; parent = parent.getParent()) configuredLimit = parent.getLevel();
		
		// Is it published?
		if(configuredLimit == null) return false;
		return configuredLimit.intValue() <= toPublish.intValue();
	}
}
