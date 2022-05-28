package rebound.db.sql.simple.api;

import java.io.Closeable;
import java.util.Iterator;
import javax.annotation.Nonnegative;
import javax.annotation.Signed;
import javax.annotation.concurrent.NotThreadSafe;
import rebound.annotations.semantic.FunctionalityType;
import rebound.annotations.semantic.StaticTraitPredicate;
import rebound.annotations.semantic.TraitPredicate;
import rebound.annotations.semantic.temporal.ConstantReturnValue;
import rebound.annotations.semantic.temporal.IdempotentOperation;
import rebound.util.collections.SimpleIterator;

/**
 * A huge difference between random access results is that they always need to be explicitly {@link #close() closed}, whereas non-random-access results autoclose upon reaching EOF!!
 */
@FunctionalityType
@NotThreadSafe
public interface SimpleSQLRandomAccessResults
extends SimpleSQLResults, Closeable
{
	/**
	 * This is -1 for BOF, {@link #getRowCount()} for EOF, and between them for a valid row you can use {@link #get(int)} on!
	 * 
	 * Since we're a {@link Iterator}/{@link SimpleIterator}, there is a "BOF" condition where {@link #next()}/{@link #nextrp()} hasn't been called yet.
	 * Which is fine for a normal iterator (and fine if you use a {@link SimpleSQLResults} as that), but since you can access the current row through method
	 * calls instead of just the return value of {@link #next()}/{@link #nextrp()}, there is such a condition as "BOF" (aka Before First) where you need to
	 * call {@link #next()}/{@link #nextrp()} at least once (or {@link #seek(long) seek}(0)), to get to the first row.  Unless this is empty, in which case
	 * that's EOF!
	 * 
	 * There are always valid BOF and EOF states, even for an empty result with no rows (-1 and 0, which is {@link #getRowCount()}).
	 */
	@Signed
	public long getCurrentRowIndex();
	
	@ConstantReturnValue
	@Nonnegative
	public long getRowCount();
	
	/**
	 * + Note that these are always guaranteed to be constant time:
	 * 		• Seeking to the next row ({@link #getCurrentRowIndex()}+1) is always just like calling {@link #next()} unless it's already EOF in which case that's an {@link IndexOutOfBoundsException}
	 * 		• Seeking to EOF (constant time because unlike seeking to the last row, nothing needs to be read! it can just remember that it's at EOF and then not return any data from {@link #get(int)})
	 * 
	 * + These are almost always constant time, but it depends on the underlying backing database:
	 * 		• Seeking to BOF
	 * 		• Seeking to the first row, 0  (which is constant time in that it's simply the same as seeking to BOF then calling {@link #next()} always 1 time which is, of course, a constant number of times XD ), unless it's empty and there are no rows in which case it's an {@link IndexOutOfBoundsException}
	 * 
	 * @param rowIndex  just what {@link #getCurrentRowIndex()} will return after this
	 * @throws IndexOutOfBoundsException  if the given index is exactly this:  <code>rowIndex < -1 || rowIndex > {@link #getRowCount()}</code>   (because BOF and EOF are valid values!)
	 */
	@IdempotentOperation
	public void seek(@Signed long rowIndex) throws IndexOutOfBoundsException;
	
	public default void seekToBOF()
	{
		seek(-1);
	}
	
	public default void seekToEOF()
	{
		seek(getRowCount());
	}
	
	
	
	@Override
	public default void drain()
	{
		seek(getRowCount());
	}
	
	public default boolean isBOF()
	{
		return getCurrentRowIndex() == -1;
	}
	
	public default boolean isEOF()
	{
		return getCurrentRowIndex() == getRowCount();
	}
	
	public default boolean isEmpty()
	{
		return getRowCount() == 0;
	}
	
	
	
	
	
	
	@TraitPredicate
	public default boolean isRandomAccess()
	{
		return true;
	}
	
	@StaticTraitPredicate
	public static boolean is(Object x)
	{
		return x instanceof SimpleSQLRandomAccessResults && ((SimpleSQLRandomAccessResults)x).isRandomAccess();
	}
	
	
	
	/**
	 * This is much less important to call for these than for other I/O resources (like files), because SimpleSQL is entirely transactional, and this is automatically closed at the end of the transaction (or sooner, if the {@link SimpleSQLPreparedQuery} is reused)
	 */
	@Override
	@IdempotentOperation
	public void close();
}
