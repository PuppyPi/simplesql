package rebound.db.sql.simple.impl.basic;

import rebound.db.sql.simple.api.SimpleSQLResults;

public abstract class SimpleSQLResultsWithStandardCurrentRecordView
implements SimpleSQLResults
{
	protected final CurrentRecordView currentRecordView = new StandardCurrentRecordView(this);
	
	@Override
	public CurrentRecordView getCurrentRecordView()
	{
		return currentRecordView;
	}
}
