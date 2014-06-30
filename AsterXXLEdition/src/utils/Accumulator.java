package utils;

import java.util.ArrayList;
import java.util.List;

import com.asterdata.ncluster.sqlmr.RuntimeContract;
import com.asterdata.ncluster.sqlmr.data.ColumnDefinition;
import com.asterdata.ncluster.sqlmr.data.RowEmitter;
import com.asterdata.ncluster.sqlmr.data.RowIterator;
import com.asterdata.ncluster.sqlmr.data.ValueHolder;

/**
 * @author jb185040
 * Implement the common accumulate argument in SQL-MR functions.
 * Usually constructed during construction phase with given contract and used during row or partition processing to copy
 * columns from in put to output.
 */
public class Accumulator {
	private ArrayList<Integer> argumentIdx = new ArrayList<Integer>();
	private ArrayList<ValueHolder> valueHolders = new ArrayList<ValueHolder>();

	/**
	 * Get list of referenced columns in accumulate argument. By default, this argument is optional.
	 * @param contract
	 */
	public Accumulator(RuntimeContract contract) {
		ArrayList<String> argument = new ArrayList<String>();
		if (contract.hasArgumentClause("accumulate"))
			argument.addAll(contract.useArgumentClause("accumulate").getValues());
		
		// Get idx for columns for faster access
		for(String accumulateColumn : argument)
			argumentIdx.add(contract.getInputInfo().getColumnIndex(accumulateColumn));
		
		// Collect types of columns for later reuse
		for(int i : argumentIdx)
			valueHolders.add(new ValueHolder(contract.getInputInfo().getColumnType(i)));
	}
	
	/**
	 * Add columns referenced in accumulate argument to the output schema.
	 * @param contract
	 * @param outputColumns
	 */
	public void constructOutputSchema(RuntimeContract contract, List<ColumnDefinition> outputColumns) {
		for(int accumulateIdx : argumentIdx)
			outputColumns.add(contract.getInputInfo().getColumns().get(accumulateIdx));
	}

	/**
	 * Read columns referenced in accumulate argument and put them to the current output row. The method does not call emitRow.
	 * @param inputIterator with row and column values to copy to output. If this is null, the last call values are used.
	 * 						If this is the first call to emit, nulls are copied to the output rows.
	 * @param outputEmitter
	 */
	public void emit(RowIterator inputIterator, RowEmitter outputEmitter) {
		for(int idx : argumentIdx) {
			if(inputIterator != null) inputIterator.getValueAt(idx, valueHolders.get(idx));
			outputEmitter.addValue(valueHolders.get(idx));
		}
	}
}
