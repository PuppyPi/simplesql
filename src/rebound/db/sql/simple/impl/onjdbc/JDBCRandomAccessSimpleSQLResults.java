package rebound.db.sql.simple.impl.onjdbc;

import static java.util.Objects.*;
import static rebound.bits.BitfieldSafeCasts.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import rebound.db.sql.UncheckedSQLException;
import rebound.db.sql.simple.api.SimpleSQLRandomAccessResults;
import rebound.db.sql.simple.impl.basic.SimpleSQLResultsWithStandardCurrentRecordView;
import rebound.exceptions.ImpossibleException;

public class JDBCRandomAccessSimpleSQLResults
extends SimpleSQLResultsWithStandardCurrentRecordView
implements SimpleSQLRandomAccessResults
{
	protected ResultSet underlying;
	protected long rowCount = -1;  //-1 = unknown as-yet
	protected int columnCount = -1;  //-1 = unknown as-yet
	protected boolean eofIfEmpty = false;  //JDBC apparently has trouble distinguishing BOF from EOF for empty ResultSets
	
	/**
	 * @param underlying must not be in the afterLast() state! but can be in any other state.
	 */
	public JDBCRandomAccessSimpleSQLResults(ResultSet underlying)
	{
		try
		{
			if (underlying.getType() == ResultSet.TYPE_FORWARD_ONLY)
				throw new IllegalArgumentException("SimpleSQL \"RandomAccess\" is JDBC \"Scrollable\"!");
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
		
		this.underlying = requireNonNull(underlying);
	}
	
	@Override
	public boolean isRandomAccess()
	{
		return true;  //for simplicity, this implementation of RandomAccessSimpleSQLResults requires this, it's not runtime-determinable!  use JDBCSimpleSQLResults for forward-only result sets!
	}
	
	
	
	@Override
	public long getCurrentRowIndex()
	{
		try
		{
			long n = getRowCount();
			
			if (n == 0)
				return eofIfEmpty ? 0 : -1;
			else
				return underlying.isAfterLast() ? n : (underlying.getRow() - 1);  //JDBC uses 1-based "row ids" but we use 0-based "indexes"
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public void seek(long rowIndex64)
	{
		int rowIndex = safeCastS64toS32(rowIndex64);
		
		try
		{
			underlying.absolute(rowIndex + 1);
			eofIfEmpty = rowIndex >= 0;
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	
	
	@Override
	public long getRowCount()
	{
		if (rowCount == -1)
		{
			try
			{
				int r = underlying.getRow();
				
				try
				{
					// https://stackoverflow.com/a/10139221
					rowCount = underlying.last() ? underlying.getRow() : 0;
				}
				finally
				{
					underlying.absolute(r);
				}
			}
			catch (SQLException exc)
			{
				throw new UncheckedSQLException(exc);
			}
		}
		
		return rowCount;
	}
	
	@Override
	public int getColumnCount()
	{
		if (columnCount == -1)
		{
			try
			{
				columnCount = underlying.getMetaData().getColumnCount();
				
				if (columnCount == -1)
					throw new ImpossibleException();
			}
			catch (SQLException exc)
			{
				throw new UncheckedSQLException(exc);
			}
		}
		
		return columnCount;
	}
	
	
	@Override
	public boolean next() throws IllegalStateException
	{
		if (isEOF())
			throw new IllegalStateException("cursor is past the end");
		
		try
		{
			boolean wasBOF = underlying.isBeforeFirst();
			
			boolean gotOne = underlying.next();
			
			if (!gotOne)
			{
				rowCount = wasBOF ? 0 : underlying.getRow();
			}
			
			eofIfEmpty = true;
			
			return gotOne;
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	
	
	
	
	
	@Override
	public Object get(int columnIndex) throws IllegalStateException
	{
		if (isBOF())
			throw new IllegalStateException("cursor is before the beginning (index = -1)");
		if (isEOF())
			throw new IllegalStateException("cursor is past the end (index = length)");
		
		try
		{
			return underlying.getObject(columnIndex + 1);
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	
	
	@Override
	public void close()
	{
		try
		{
			underlying.close();
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
}
