package tests;

import utils.ErrorHandler;
import utils.Utils;

import com.asterdata.ncluster.sqlmr.Drainable;
import com.asterdata.ncluster.sqlmr.OutputInfo;
import com.asterdata.ncluster.sqlmr.PartitionFunction;
import com.asterdata.ncluster.sqlmr.RowFunction;
import com.asterdata.ncluster.sqlmr.RuntimeContract;
import com.asterdata.ncluster.sqlmr.data.PartitionDefinition;
import com.asterdata.ncluster.sqlmr.data.RowEmitter;
import com.asterdata.ncluster.sqlmr.data.RowIterator;

public final class ErrorHandlerTest implements RowFunction, PartitionFunction, Drainable {
	private ErrorHandler errorHandler = null;
	
	public ErrorHandlerTest(RuntimeContract contract) {
		errorHandler = new ErrorHandler(
				this.getClass().getName(),
				Utils.getSingleBooleanFromParameter(contract, "stoponerror", false),
				Utils.getSingleIntFromParameter(contract, "log", Integer.MAX_VALUE));
		contract.setOutputInfo(new OutputInfo(contract.getInputInfo().getColumns()));
		contract.complete();
	}

	public void operateOnSomeRows(RowIterator inputIterator, RowEmitter outputEmitter) {
		int i = 0;
		while (inputIterator.advanceToNextRow()) {
			i++;
			errorHandler.enterOperateOnRow(inputIterator, outputEmitter);
			
			try {
				if(i == 3) throw new Exception("Test-3!");
			} catch (Exception e) {
				errorHandler.catchException(e);
			}
			
			try {
				if(i == 13) throw new Exception("Test-13!");
			} catch (Exception e) {
				errorHandler.catchException(e, "extra1-1");
			}
			
			try {
				if(i == 23) throw new Exception("Test-23!");
			} catch (Exception e) {
				errorHandler.catchException(e, "extra2-1", "extra2-2");
			}

			if(i == 5) errorHandler.skipRow();
			if(i == 7) errorHandler.skipRow();

			errorHandler.exitOperateOnRow();
		}
	}

	public void operateOnPartition(PartitionDefinition definition, RowIterator inputIterator, RowEmitter outputEmitter) {
		int i = 0;
		errorHandler.enterOperateOnPartition(definition, inputIterator, outputEmitter);
		while (inputIterator.advanceToNextRow()) {
			i++;
			errorHandler.enterOperateOnRow(inputIterator, outputEmitter);
			
			try {
				if(i == 3) throw new Exception("Test-3!");
			} catch (Exception e) {
				errorHandler.catchException(e);
			}
			
			try {
				if(i == 13) throw new Exception("Test-13!");
			} catch (Exception e) {
				errorHandler.catchException(e, "extra1-1");
			}
			
			try {
				if(i == 23) throw new Exception("Test-23!");
			} catch (Exception e) {
				errorHandler.catchException(e, "extra2-1", "extra2-2");
			}
			
			errorHandler.exitOperateOnRow();
		}
		errorHandler.exitOperateOnPartition();
	}

	public void drainOutputRows(RowEmitter outputEmitter) {
		errorHandler.drainOutputRows();
	}
}
