package rebound.db.sql.simple.impl.onjdbc;

import static java.util.Objects.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import rebound.db.sql.UncheckedSQLException;
import rebound.db.sql.simple.api.SimpleSQLResults;
import rebound.db.sql.simple.impl.basic.SimpleSQLResultsWithStandardCurrentRecordView;
import rebound.exceptions.ImpossibleException;

public class JDBCSimpleSQLResults
extends SimpleSQLResultsWithStandardCurrentRecordView
implements SimpleSQLResults
{
	protected ResultSet underlying;
	protected int columnCount = -1;  //-1 = unknown as-yet
	protected boolean bof = true;
	protected boolean eof = false;
	
	public JDBCSimpleSQLResults(ResultSet underlying)
	{
		this.underlying = requireNonNull(underlying);
	}
	
	
	@Override
	public boolean next() throws IllegalStateException
	{
		if (eof)
			throw new IllegalStateException("cursor is past the end");
		
		try
		{
			boolean gotOne = underlying.next();
			
			bof = false;  //regardless of gotOne
			
			if (!gotOne)
			{
				eof = true;
				
				try
				{
					getColumnCount();  //do this before we close the underlying ResultSet just in case we haven't cached it yet but it turns out we need it later on X3
				}
				finally
				{
					underlying.close();
				}
				
				underlying = null;  //for GC :>
			}
			
			return gotOne;
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	
	
	@Override
	public void drain()
	{
		if (!eof)
		{
			try
			{
				eof = true;
				
				try
				{
					getColumnCount();  //do this before we close the underlying ResultSet just in case we haven't cached it yet but it turns out we need it later on X3
				}
				finally
				{
					underlying.close();
				}
				
				underlying = null;  //for GC :>
			}
			catch (SQLException exc)
			{
				throw new UncheckedSQLException(exc);
			}
		}
	}
	
	
	
	@Override
	public Object get(int columnIndex) throws IllegalStateException
	{
		if (bof)
			throw new IllegalStateException("next() has not yet been called (which must be called once even if this is empty..which is how you tell it's empty!)");
		if (eof)
			throw new IllegalStateException("cursor is past the end");
		
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
	public boolean isBOF()
	{
		return bof;
	}
	
	@Override
	public boolean isEOF()
	{
		return eof;
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
}
