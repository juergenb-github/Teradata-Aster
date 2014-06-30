package sqlmrFunctions;

import java.util.ArrayList;
import java.util.List;

import utils.Accumulator;
import utils.ErrorHandler;
import utils.Utils;

import com.asterdata.ncluster.sqlmr.Drainable;
import com.asterdata.ncluster.sqlmr.IllegalUsageException;
import com.asterdata.ncluster.sqlmr.OutputInfo;
import com.asterdata.ncluster.sqlmr.PartitionFunction;
import com.asterdata.ncluster.sqlmr.RuntimeContract;
import com.asterdata.ncluster.sqlmr.data.ColumnDefinition;
import com.asterdata.ncluster.sqlmr.data.PartitionDefinition;
import com.asterdata.ncluster.sqlmr.data.RowEmitter;
import com.asterdata.ncluster.sqlmr.data.RowIterator;
import com.asterdata.ncluster.sqlmr.data.SqlType;

/**
 * @author Juergen Boiselle
 *
 * Returns the significance level, or p-value, associated with a Chi-square goodness of fit test comparing the frequency counts
 * of two list of values (observed and expected). The number returned is the smallest significance level at which one can reject
 * the null hypothesis that the observed counts conform to the frequency distribution described by the expected counts.
 * Observed and expected counts do not need to have the same base nor need to sum up to the same value.
 * The Chi-square goodness of fit test compares two sets of values and calculates the probability that both sets are from the same distribution.
 * 
 * Syntax:
 * 	SELECT * FROM ChiSquareTest(
 * 		ON { table_name | view_name | (query) }
 * 		PARTITION BY partition_column1, ... [ORDER BY chunk]
 * 		EXPECTED('column_name')
 * 		OBSERVED('column_name')
 * 		[accumulate('passed_thru_column_name'[, ...])]
 * 		[stopOnError('true'|'false')]
 * 		[log(infoN)]
 * 
 * Parameter:
 * EXPECTED	mandatory	Name of the column in the ON Phrase that contains the expected set of values.
 * 						Expected counts must all be non-null and positive. Must be any numeric type.
 * OBSERVED	mandatory	Name of the column in the ON Phrase that contains the observed set of values.
 * 						Observed counts must all be non-null and ≥ 0. Must be integer.
 * accumulate	optional	Copy columns of input to each output row.
 * stopOnError	optional	Possible values are 'true' and 'false'. If set to 'true' the function stops when it occurs an error while
 * 							reading the partitions. 'false' indicates, that the function continues, ignoring the partition and continuing
 * 							with the next one. Default is 'false'.
 * 							The exception along with information about the partition is written to the log and returned to the user (stopOnError=true).
 * log optional	number of rows between information logged. If set, every number of rows or partitions, information is logged at
 * 							level INFO about timings and progress.	
 * 
 * Each partition must contain at least two rows of values.
 * 
 * Output:
 * The result is one p-value (probability) per partition. Simply spoken the p-value is the probability, that the observed set of values has the same
 * distribution as the expected values - without knowing the distribution. Using the same values for observed and expected results in a
 * p-value = 100%. 
 *
 * pValue	double	p-value of a Chi-square goodness of fit test.
 * 
 * Error handling:
 * - Whenever a Java exception is thrown such as ZipException or an IOException, the partition is aborted.
 * - The exception and the stack is printed in the Aster log file. Processing continues with next partition.
 * 
 * Possible Java Exception:
 * - NotPositiveException - if observed has negative entries
 * - NotStrictlyPositiveException - if expected has entries that are not strictly positive
 * - DimensionMismatchException - if the arrays length is less than 2
 * - MaxCountExceededException - if an error occurs computing the p-value
 *  
 * TODO: Implement
 * TODO: Test cases for different sets
 * TODO: Implement with Benford data
 * TODO: Documentation with Syntax, parameter explanation, output explanation, error messages, examples of call, examples how to work with result
 * TODO: Tell CoE (Connections) about this function
 */
public final class ChiSquareTest implements PartitionFunction, Drainable {
	// These member variables will be populated with the values of the argument clauses passed to the SQL-MR function.
	private int observedArgumentIdx = 0;
	private int expectedArgumentIdx = 0;
	private org.apache.commons.math3.stat.inference.ChiSquareTest chiSquareTest = null;
	private Accumulator accumulator = null;
	private ErrorHandler errorHandler = null;
	
	/* 
	 * The constructor establishes the RuntimeContract between
	 * the SQL-MR function and Aster Database. During query planning,
	 * the function will constructed on a single node. During
	 * query execution, it will be constructed and run on one or more nodes.
	 */
	public ChiSquareTest(RuntimeContract contract) {
		// Read argument clauses into appropriate member variables.
		observedArgumentIdx = contract.getInputInfo().getColumnIndex(contract.useArgumentClause("observed").getSingleValue());
		expectedArgumentIdx = contract.getInputInfo().getColumnIndex(contract.useArgumentClause("expected").getSingleValue());
		accumulator = new Accumulator(contract);
		errorHandler = new ErrorHandler(
				this.getClass().getName(),
				Utils.getSingleBooleanFromParameter(contract, "stoponerror", false),
				Utils.getSingleIntFromParameter(contract, "log", Integer.MAX_VALUE));
		
		// Initialize the service
		chiSquareTest = new org.apache.commons.math3.stat.inference.ChiSquareTest();

		// Verify that the function accepts the given input schema.
		if(!SqlType.integer().equals(contract.getInputInfo().getColumnType(observedArgumentIdx))) {
			throw new IllegalUsageException("\"" + contract.useArgumentClause("observed").getSingleValue() + "\"  must be of type 'integer'");
		}
		if(!SqlType.doublePrecision().equals(contract.getInputInfo().getColumnType(expectedArgumentIdx))) {
			throw new IllegalUsageException("\"" + contract.useArgumentClause("expected").getSingleValue() + "\"  must be a numeric type");
		}

		// Construct the output schema
		List<ColumnDefinition> outputColumns = new ArrayList<ColumnDefinition>();
		accumulator.constructOutputSchema(contract, outputColumns);
		outputColumns.add(new ColumnDefinition("pValue", SqlType.getType("double")));
		contract.setOutputInfo(new OutputInfo(outputColumns));

		// Complete the contract
		contract.complete();
	}

	public void operateOnPartition(PartitionDefinition definition, RowIterator inputIterator, RowEmitter outputEmitter) {
		errorHandler.enterOperateOnPartition(definition, inputIterator, outputEmitter);

		try {
			// Collect input rows for observed and expected values
			ArrayList<Double> expectedList = new ArrayList<Double>();
			ArrayList<Long> observedList = new ArrayList<Long>();
			
			while (inputIterator.advanceToNextRow()) {
				errorHandler.enterOperateOnRow(inputIterator, outputEmitter);
				if(inputIterator.isNullAt(observedArgumentIdx) || inputIterator.isNullAt(expectedArgumentIdx))
					throw new IllegalArgumentException("observed and expected values cannot be null");
				
				expectedList.add(inputIterator.getDoubleAt(expectedArgumentIdx));
				observedList.add(inputIterator.getLongAt(observedArgumentIdx));
				errorHandler.exitOperateOnRow();
			}
			
			double[] expected = new double[expectedList.size()];
			for(int i=0; i<expected.length; i++) expected[i] = expectedList.get(i);
			
			long[] observed = new long[observedList.size()];
			for(int i=0; i<observed.length; i++) observed[i] = observedList.get(i);
			
			// Run test
			double pValue = chiSquareTest.chiSquareTest(expected, observed);
			
			// Emit result
			accumulator.emit(inputIterator, outputEmitter);
			outputEmitter.addDouble(pValue);
			outputEmitter.emitRow();
		} catch(IllegalArgumentException e) {
			errorHandler.catchException(e);
			return; // End this partition and go to next if stopOnError is set to false (otherwise exception is thrown)
		}

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
}
