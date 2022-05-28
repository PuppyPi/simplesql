package rebound.db.sql.simple.api;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public interface SimpleSQLPreparedUpdate
{
	/**
	 * (note that this calls {@link #executeQueue()} implicitly if there is a queue)
	 */
	public void executeUpdate(Object... parameterValues);
	
	
	
	
	/**
	 * Using the queue for bulk updates can improve performance.
	 * 
	 * NOTE: This appears not to work for inserts (with H2) ?!?!?!?
	 * 		—Sean, 2022-01-05 02:31:35 z
	 */
	public void queueUpdate(Object... parameterValues);
	
	public int getQueueSize();
	
	/**
	 * Silently does nothing if there's no queue.
	 */
	public void executeQueue();
	
	public default boolean hasQueue()
	{
		return getQueueSize() != 0;
	}
}
