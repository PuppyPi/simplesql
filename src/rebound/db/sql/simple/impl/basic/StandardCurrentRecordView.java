package rebound.db.sql.simple.impl.basic;

import static java.util.Objects.*;
import java.util.List;
import rebound.db.sql.simple.api.SimpleSQLResults;
import rebound.db.sql.simple.api.SimpleSQLResults.CurrentRecordView;
import rebound.util.collections.CollectionUtilities;
import rebound.util.collections.DefaultReadonlyList;

public class StandardCurrentRecordView
implements CurrentRecordView, DefaultReadonlyList<Object>
{
	protected final SimpleSQLResults containingResults;
	
	public StandardCurrentRecordView(SimpleSQLResults containingResults)
	{
		this.containingResults = requireNonNull(containingResults);
	}
	
	
	
	@Override
	public SimpleSQLResults getContainingResults()
	{
		return containingResults;
	}
	
	@Override
	public Object get(int index)
	{
		return containingResults.get(index);
	}
	
	@Override
	public int size()
	{
		return containingResults.getColumnCount();
	}
	
	
	
	
	@Override
	public boolean equals(Object obj)
	{
		if (obj instanceof List)
			return CollectionUtilities.defaultListsEquivalent(this, (List)obj);
		else
			return false;
	}
	
	public int hashCode()
	{
		return CollectionUtilities.defaultListHashCode(this);
	}
	
	@Override
	public String toString()
	{
		return this._toString();
	}
}
