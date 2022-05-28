package rebound.db.sql.simple.impl.onjdbc;

import static java.util.Objects.*;
import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import rebound.db.sql.UncheckedSQLException;
import rebound.db.sql.simple.api.SimpleSQLConnection;
import rebound.db.sql.simple.api.SimpleSQLPreparedQuery;
import rebound.db.sql.simple.api.SimpleSQLPreparedUpdate;
import rebound.db.sql.simple.api.SimpleSQLRandomAccessResults;
import rebound.db.sql.simple.api.SimpleSQLResults;
import rebound.db.sql.simple.impl.snapshotresults.custombinary.CustomBinarySnapshotUtilities;
import rebound.util.functional.throwing.FunctionalInterfacesThrowingCheckedExceptionsStandard.NullaryFunctionThrowingIOException;

public class JDBCSimpleSQLConnection
implements SimpleSQLConnection
{
	protected final Connection underlying;
	protected NullaryFunctionThrowingIOException<File> temporaryFileMakerForSnapshotQueries;
	
	public JDBCSimpleSQLConnection(Connection underlying)
	{
		this(underlying, CustomBinarySnapshotUtilities.DefaultTemporaryFileMaker);
	}
	
	public JDBCSimpleSQLConnection(Connection underlying, NullaryFunctionThrowingIOException<File> temporaryFileMakerForSnapshotQueries)
	{
		this.underlying = requireNonNull(underlying);
		this.temporaryFileMakerForSnapshotQueries = temporaryFileMakerForSnapshotQueries;
		
		try
		{
			underlying.setAutoCommit(false);
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	
	
	@Override
	public void executeUpdate(String sql)
	{
		try
		{
			underlying.createStatement().execute(sql);  //H2 doesn't like executeUpdate() for the "SCRIPT" command!
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public SimpleSQLPreparedUpdate prepareUpdate(String sql)
	{
		try
		{
			return new JDBCSimpleSQLPreparedUpdate(underlying.prepareStatement(sql));
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	
	
	@Override
	public SimpleSQLResults executeQuery(String sql)
	{
		try
		{
			return new JDBCSimpleSQLResults(underlying.createStatement().executeQuery(sql));
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public SimpleSQLResults executeRandomAccessQuery(String sql)
	{
		try
		{
			return new JDBCRandomAccessSimpleSQLResults(underlying.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery(sql));
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public SimpleSQLRandomAccessResults executeSnapshotQuery(String sql)
	{
		return CustomBinarySnapshotUtilities.performLocallyBufferedSnapshotting(executeQuery(sql));
	}
	
	@Override
	public SimpleSQLPreparedQuery prepareQuery(String sql)
	{
		try
		{
			return new JDBCSimpleSQLPreparedQuery(underlying.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY), underlying.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY), temporaryFileMakerForSnapshotQueries);
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	
	
	
	@Override
	public void commit()
	{
		try
		{
			underlying.commit();
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public void rollback()
	{
		try
		{
			underlying.rollback();
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
