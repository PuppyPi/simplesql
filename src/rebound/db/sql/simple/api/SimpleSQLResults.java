package rebound.db.sql.simple.api;

import java.util.Iterator;
import java.util.List;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import rebound.annotations.hints.IntendedToNOTBeSubclassedImplementedOrOverriddenByApiUser;
import rebound.annotations.hints.IntendedToOptionallyBeSubclassedImplementedOrOverriddenByApiUser;
import rebound.annotations.semantic.temporal.ConstantReturnValue;
import rebound.annotations.semantic.temporal.IdempotentOperation;
import rebound.db.sql.simple.api.SimpleSQLResults.CurrentRecordView;
import rebound.exceptions.StopIterationReturnPath;
import rebound.exceptions.StructuredClassCastException;
import rebound.util.collections.SimpleIterator;

/**
 * Note that the returned {@link CurrentRecordView} will always be the same Java object instance!  (particularly important when this is used as a {@link SimpleIterator}!!)
 * Which record is the current record simply changes upon calling {@link Iterator#next()}.
 * 
 * BOF and EOF work the same way as with {@link SimpleSQLRandomAccessResults}, so see the Javadoc on {@link SimpleSQLRandomAccessResults#getCurrentRowIndex()} for a detailed discussion on BOF and EOF states.
 * (but suffice it to say, those are both always valid and always different states, and BOF is the initial state you can always call {@link #next()} on, EOF is the final state you can't call {@link #next()} on, and you can't call {@link #get(int)} on either one!)
 */
@NotThreadSafe
public interface SimpleSQLResults
extends SimpleIterator<CurrentRecordView>
{
	/**
	 * @return true  if we have a new Current Record, otherwise we've hit EOF and there is no Current Record (which happens after the first call to this on empty results)
	 * @throws IllegalStateException  if this is called while we're already at an EOF condition  (ie, the last call to next() returned false, as opposed to this call transitions *into* an EOF condition by returning false)
	 */
	public boolean next() throws IllegalStateException;
	
	/**
	 * @param columnIndex zero-based!
	 * @throws IllegalStateException on BOF or EOF condition (before the first next() or after the last)
	 */
	public Object get(int columnIndex) throws IllegalStateException;
	
	/**
	 * Always works, regardless of BOF or EOF state or even if it's empty with no records :3
	 */
	@ConstantReturnValue
	public int getColumnCount();
	
	
	/**
	 * Does nothing if already at EOF.
	 * This always leaves the iterator at EOF condition after returning.
	 */
	@Override
	@IdempotentOperation
	public void drain();
	
	
	
	public boolean isBOF();
	
	public boolean isEOF();
	
	
	
	
	
	
	//////////// Syntactic sugar X3 ////////////
	
	@ConstantReturnValue
	public CurrentRecordView getCurrentRecordView();
	
	
	public static interface CurrentRecordView
	extends List<Object>
	{
		@ConstantReturnValue
		public SimpleSQLResults getContainingResults();
		
		/**
		 * @see SimpleSQLResults#get(int)
		 */
		@Override
		public Object get(int index);
		
		/**
		 * Convenience method for converting {@link Byte} and {@link Short} to {@link Integer}
		 */
		@IntendedToOptionallyBeSubclassedImplementedOrOverriddenByApiUser
		public default @Nullable Integer getAsInt(int index) throws ClassCastException
		{
			Object v = get(index);
			
			if (v == null)
				return null;
			else if (v instanceof Byte)
				return (int)(Byte)v;
			else if (v instanceof Short)
				return (int)(Short)v;
			else if (v instanceof Integer)
				return (Integer)v;
			else
				throw new StructuredClassCastException(v.getClass());
		}
		
		/**
		 * Convenience method for converting {@link Byte}, {@link Short} and {@link Integer} to {@link Long}
		 */
		@IntendedToOptionallyBeSubclassedImplementedOrOverriddenByApiUser
		public default @Nullable Long getAsLong(int index) throws ClassCastException
		{
			Object v = get(index);
			
			if (v == null)
				return null;
			else if (v instanceof Byte)
				return (long)(Byte)v;
			else if (v instanceof Short)
				return (long)(Short)v;
			else if (v instanceof Integer)
				return (long)(Integer)v;
			else if (v instanceof Long)
				return (Long)v;
			else
				throw new StructuredClassCastException(v.getClass());
		}
		
		/**
		 * @see SimpleSQLResults#getColumnCount()
		 */
		@Override
		public int size();
	}
	
	
	
	
	
	/**
	 * Note that the returned {@link CurrentRecordView} will always be the same Java object instance!
	 * Which record is the current record simply changes upon calling {@link Iterator#next()}.
	 */
	@IntendedToNOTBeSubclassedImplementedOrOverriddenByApiUser
	public default Iterator<CurrentRecordView> asIterator()
	{
		return SimpleIterator.defaultToIterator(this);
	}
	
	/**
	 * Note that the returned {@link CurrentRecordView} will always be the same Java object instance!
	 * Which record is the current record simply changes upon calling {@link Iterator#next()}.
	 */
	@Override
	public default CurrentRecordView nextrp() throws StopIterationReturnPath
	{
		CurrentRecordView v = getCurrentRecordView();
		
		if (!SimpleSQLResults.this.next())
			throw StopIterationReturnPath.I;
		
		return v;
	}
}
