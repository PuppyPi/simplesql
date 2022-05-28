package rebound.db.sql.simple.impl.onjdbc;

import static java.util.Objects.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import rebound.db.sql.UncheckedSQLException;
import rebound.db.sql.simple.api.SimpleSQLPreparedUpdate;

public class JDBCSimpleSQLPreparedUpdate
implements SimpleSQLPreparedUpdate
{
	protected final PreparedStatement underlying;
	protected int queueSize = 0;
	
	public JDBCSimpleSQLPreparedUpdate(PreparedStatement underlying)
	{
		this.underlying = requireNonNull(underlying);
	}
	
	
	@Override
	public void executeUpdate(Object... parameterValues)
	{
		try
		{
			if (queueSize != 0)
				this.executeQueue();
			
			JDBCSimpleSQLUtilities.setPreparedStatementParameters(underlying, parameterValues);
			underlying.execute();  //Note H2 doesn't like executeUpdate() for some commands, like "SCRIPT"!   (I'm not sure if that can even be used in a PreparedStatement, but it doesn't hurt to do it this way :3 )
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public void queueUpdate(Object... parameterValues)
	{
		try
		{
			JDBCSimpleSQLUtilities.setPreparedStatementParameters(underlying, parameterValues);
			underlying.addBatch();
			queueSize++;  //if above is successful :3
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public int getQueueSize()
	{
		return queueSize;
	}
	
	@Override
	public void executeQueue()
	{
		if (queueSize != 0)
		{
			try
			{
				underlying.execute();
				queueSize = 0;  //if above is successful :3
			}
			catch (SQLException exc)
			{
				throw new UncheckedSQLException(exc);
			}
		}
	}
}
