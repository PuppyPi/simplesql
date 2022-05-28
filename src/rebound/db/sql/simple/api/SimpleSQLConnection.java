package rebound.db.sql.simple.api;

import static rebound.io.util.BasicIOUtilities.*;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import rebound.annotations.hints.IntendedToNOTBeSubclassedImplementedOrOverriddenByApiUser;
import rebound.annotations.semantic.allowedoperations.ReadonlyValue;
import rebound.annotations.semantic.reachability.SnapshotValue;
import rebound.db.SimpleTransactionManager;
import rebound.exceptions.NotFoundException;
import rebound.exceptions.NotSingletonException;

@NotThreadSafe
public interface SimpleSQLConnection
extends SimpleTransactionManager, Closeable
{
	public void executeUpdate(String sql);  //See SimpleSQLPreparedUpdate.executeUpdate()
	public SimpleSQLPreparedUpdate prepareUpdate(String sql);
	
	
	
	
	/**
	 * The return value is guaranteed not to be {@link Closeable} or {@link SimpleSQLRandomAccessResults}.  It will auto-close upon reaching the end :3
	 */
	public SimpleSQLResults executeQuery(String sql);
	
	public SimpleSQLResults executeRandomAccessQuery(String sql);
	
	/**
	 * Say you have a FREAKISHLY LARGE query.
	 * Even if it's possible to do it normally, you don't want to block the entire transaction while you stream in its data.
	 * So you do this!  And then the data will be written to a temporary file or stored in memory (if it's small enough),
	 * then you can call {@link SimpleSQLConnection#commit()}, and *then* read the {@link SimpleSQLResults} and its contents won't
	 * be affected by other transactions because it's an (atomic) snapshot of the data! :D
	 */
	public SimpleSQLRandomAccessResults executeSnapshotQuery(String sql);  //See SimpleSQLPreparedQuery.executeSnapshotQuery()
	
	
	public SimpleSQLPreparedQuery prepareQuery(String sql);
	
	
	
	
	@Override
	public void close();
	
	
	
	
	
	
	
	
	
	
	
	
	
	//////////// Syntactic sugar X3 ////////////
	
	/**
	 * @return one row of data; indexes correspond to columns :3    it's a {@link SnapshotValue}, so unless there are blobs/clobs, it's safe regardless of database closing and such :3
	 * @throws NotFoundException if there was less than one (zero) row!
	 * @throws NotSingletonException if there was more than one row!
	 */
	@IntendedToNOTBeSubclassedImplementedOrOverriddenByApiUser
	public default @Nonnull List<Object> queryAsSingleton(String sql) throws NotFoundException, NotSingletonException
	{
		List<Object> r = queryAsSingletonOrNull(sql);
		if (r == null)
			throw new NotFoundException();
		else
			return r;
	}
	
	/**
	 * @return null if there were no rows or one row of data; indexes correspond to columns :3    it's a {@link SnapshotValue}, so unless there are blobs/clobs, it's safe regardless of database closing and such :3
	 * @throws NotSingletonException if there was more than one row!
	 */
	@SnapshotValue
	@ReadonlyValue
	@IntendedToNOTBeSubclassedImplementedOrOverriddenByApiUser
	public default @Nullable List<Object> queryAsSingletonOrNull(String sql) throws NotSingletonException
	{
		SimpleSQLResults r = executeQuery(sql);
		
		if (r.next())
		{
			List<Object> l = Arrays.asList(r.getCurrentRecordView().toArray());
			
			if (r.next())
			{
				r.drain();
				
				if (r instanceof Closeable)
					closeWithoutError((Closeable)r);
				
				throw new NotSingletonException();
			}
			else
			{
				if (r instanceof Closeable)
					closeWithoutError((Closeable)r);
				
				return l;
			}
		}
		else
		{
			return null;
		}
	}
}
