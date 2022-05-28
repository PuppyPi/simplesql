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
import rebound.exceptions.NotFoundException;
import rebound.exceptions.NotSingletonException;

/**
 * Note that update+query combinations are considered just queries here.  (Analogous to how "i++" is an r-value expression like "i+1" :3 )
 * 
 * Note that the return value of {@link #executeQuery(Object...)} is only valid for the current transaction (like the one from {@link SimpleSQLConnection#executeQuery(String)}) but also only valid until the next time it is called!! each instance of {@link SimpleSQLPreparedQuery} can have only one {@link SimpleSQLResults} at a time!
 * {@link #executeSnapshotQuery(Object...)} has neither of these limitations :3  (like {@link SimpleSQLConnection#executeSnapshotQuery(String)})
 */
@NotThreadSafe
public interface SimpleSQLPreparedQuery
{
	public SimpleSQLResults executeQuery(Object... parameterValues);
	public SimpleSQLRandomAccessResults executeRandomAccessQuery(Object... parameterValues);
	public SimpleSQLRandomAccessResults executeSnapshotQuery(Object... parameterValues);
	
	
	
	
	
	
	
	
	
	
	//////////// Syntactic sugar X3 ////////////
	
	/**
	 * @return one row of data; indexes correspond to columns :3    it's a {@link SnapshotValue}, so unless there are blobs/clobs, it's safe regardless of database closing and such :3
	 * @throws NotFoundException if there was less than one (zero) row!
	 * @throws NotSingletonException if there was more than one row!
	 */
	@IntendedToNOTBeSubclassedImplementedOrOverriddenByApiUser
	public default @Nonnull List<Object> queryAsSingleton(Object... parameterValues) throws NotFoundException, NotSingletonException
	{
		List<Object> r = queryAsSingletonOrNull(parameterValues);
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
	public default @Nullable List<Object> queryAsSingletonOrNull(Object... parameterValues) throws NotSingletonException
	{
		SimpleSQLResults r = executeQuery(parameterValues);
		
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
