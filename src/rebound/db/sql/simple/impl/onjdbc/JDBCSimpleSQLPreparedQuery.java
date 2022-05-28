package rebound.db.sql.simple.impl.onjdbc;

import static java.util.Objects.*;
import java.io.File;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import rebound.db.sql.UncheckedSQLException;
import rebound.db.sql.simple.api.SimpleSQLPreparedQuery;
import rebound.db.sql.simple.api.SimpleSQLRandomAccessResults;
import rebound.db.sql.simple.api.SimpleSQLResults;
import rebound.db.sql.simple.impl.snapshotresults.custombinary.CustomBinarySnapshotUtilities;
import rebound.util.functional.throwing.FunctionalInterfacesThrowingCheckedExceptionsStandard.NullaryFunctionThrowingIOException;

public class JDBCSimpleSQLPreparedQuery
implements SimpleSQLPreparedQuery
{
	protected final PreparedStatement underlyingSequential;
	protected final PreparedStatement underlyingRandomAccess;
	protected NullaryFunctionThrowingIOException<File> temporaryFileMakerForSnapshotQueries;
	
	public JDBCSimpleSQLPreparedQuery(PreparedStatement underlyingSequential, PreparedStatement underlyingRandomAccess, NullaryFunctionThrowingIOException<File> temporaryFileMakerForSnapshotQueries)
	{
		this.underlyingSequential = requireNonNull(underlyingSequential);
		this.underlyingRandomAccess = requireNonNull(underlyingRandomAccess);
		this.temporaryFileMakerForSnapshotQueries = temporaryFileMakerForSnapshotQueries;
	}
	
	@Override
	public SimpleSQLResults executeQuery(Object... parameterValues)
	{
		try
		{
			JDBCSimpleSQLUtilities.setPreparedStatementParameters(underlyingSequential, parameterValues);
			
			return new JDBCSimpleSQLResults(underlyingSequential.executeQuery());
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public SimpleSQLRandomAccessResults executeRandomAccessQuery(Object... parameterValues)
	{
		try
		{
			JDBCSimpleSQLUtilities.setPreparedStatementParameters(underlyingRandomAccess, parameterValues);
			
			return new JDBCRandomAccessSimpleSQLResults(underlyingRandomAccess.executeQuery());
		}
		catch (SQLException exc)
		{
			throw new UncheckedSQLException(exc);
		}
	}
	
	@Override
	public SimpleSQLRandomAccessResults executeSnapshotQuery(Object... parameterValues)
	{
		return CustomBinarySnapshotUtilities.performLocallyBufferedSnapshotting(executeQuery(parameterValues), temporaryFileMakerForSnapshotQueries);  //Make sure to use the sequential one for this otherwise some datbases might do their own write-to-file stage!! XD''     (edit: Yesssssss!  This is exactly what H2 does and for large queries, the TCP connection set up by the JDBC driver times out!! X'D   So we absolutely *need* to buffer them to disk on the client side!)
	}
}
