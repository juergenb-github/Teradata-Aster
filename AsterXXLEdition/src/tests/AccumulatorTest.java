package tests;

import java.util.ArrayList;
import java.util.List;

import utils.Accumulator;

import com.asterdata.ncluster.sqlmr.OutputInfo;
import com.asterdata.ncluster.sqlmr.RowFunction;
import com.asterdata.ncluster.sqlmr.RuntimeContract;
import com.asterdata.ncluster.sqlmr.data.ColumnDefinition;
import com.asterdata.ncluster.sqlmr.data.RowEmitter;
import com.asterdata.ncluster.sqlmr.data.RowIterator;

public final class AccumulatorTest implements RowFunction {
	private Accumulator accumulator = null;

	public AccumulatorTest(RuntimeContract contract) {
		accumulator = new Accumulator(contract);
		List<ColumnDefinition> outputColumns = new ArrayList<ColumnDefinition>();
		accumulator.constructOutputSchema(contract, outputColumns);
		contract.setOutputInfo(new OutputInfo(outputColumns));
		contract.complete();
	}

	public void operateOnSomeRows(RowIterator inputIterator, RowEmitter outputEmitter) {
		while(inputIterator.advanceToNextRow()) {
			accumulator.emit(inputIterator, outputEmitter);
			outputEmitter.emitRow();
		}
	}
}
