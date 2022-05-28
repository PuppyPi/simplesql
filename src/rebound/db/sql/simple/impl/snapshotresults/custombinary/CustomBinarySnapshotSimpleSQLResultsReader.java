package rebound.db.sql.simple.impl.snapshotresults.custombinary;

import java.io.IOException;
import java.io.UncheckedIOException;
import javax.annotation.Nullable;
import rebound.annotations.semantic.allowedoperations.TreatAsImmutableValue;
import rebound.db.sql.simple.api.SimpleSQLRandomAccessResults;
import rebound.db.sql.simple.impl.basic.SimpleSQLResultsWithStandardCurrentRecordView;
import rebound.io.iio.RandomAccessInputByteStream;

//Todo a format with a seek-table of offsets at the end for fast random-access seek()s!    (at the end not the beginning because during writing from a forward-only resultset, you can't know how large it will need to be!)

public class CustomBinarySnapshotSimpleSQLResultsReader
extends SimpleSQLResultsWithStandardCurrentRecordView
implements SimpleSQLRandomAccessResults
{
	protected final RandomAccessInputByteStream lowlevel;
	protected final @TreatAsImmutableValue CustomSimpleSQLSupportedTypes[] columnDatatypes;
	protected final long numberOfRows;
	protected final @Nullable Runnable onClose;
	
	protected long rowIndex = -1;
	protected Object[] currentRowData;
	protected byte[] nullFlagsBuffer;
	
	/**
	 * @param numberOfRows
	 * @param lowlevel  can be null if numberOfRows == 0
	 * @param columnDatatypes  can be null if numberOfRows == 0
	 */
	public CustomBinarySnapshotSimpleSQLResultsReader(long numberOfRows, RandomAccessInputByteStream lowlevel, @TreatAsImmutableValue CustomSimpleSQLSupportedTypes[] columnDatatypes, @Nullable Runnable onClose)
	{
		this.lowlevel = lowlevel;
		this.columnDatatypes = columnDatatypes;
		this.numberOfRows = numberOfRows;
		this.onClose = onClose;
		
		this.currentRowData = numberOfRows == 0 ? null : new Object[columnDatatypes.length];
		this.nullFlagsBuffer = numberOfRows == 0 ? null : new byte[CustomBinarySnapshotFormat.sizeOfNullFlagsArray(columnDatatypes.length)];
	}
	
	@Override
	public long getRowCount()
	{
		return numberOfRows;
	}
	
	@Override
	public int getColumnCount()
	{
		return columnDatatypes.length;
	}
	
	@Override
	public long getCurrentRowIndex()
	{
		return rowIndex;
	}
	
	
	@Override
	public boolean next() throws IllegalStateException
	{
		if (isEOF())
			throw new IllegalStateException();
		else
		{
			rowIndex++;
			
			if (!isBOF())
			{
				try
				{
					CustomBinarySnapshotFormat.readRow(lowlevel, columnDatatypes, nullFlagsBuffer, currentRowData);
				}
				catch (IOException exc)
				{
					throw new UncheckedIOException(exc);
				}
			}
		}
		
		return !isEOF();
	}
	
	
	@Override
	public void seek(long rowIndex)
	{
		if (rowIndex < -1)
			throw new IndexOutOfBoundsException("seek before BOF attempted!");
		
		else if (rowIndex > getRowCount())
			throw new IndexOutOfBoundsException("seek past EOF attempted!");
		
		else if (rowIndex == getRowCount())
			this.rowIndex = rowIndex;
		
		else if (rowIndex == this.rowIndex + 1)
			next();
		
		else
		{
			try
			{
				this.rowIndex = -1;
				lowlevel.seek(0);
				
				while (this.rowIndex != rowIndex)  //if both are -1 then next() isn't called! if rowIndex is 0 then this is called once to read the data! :D
					next();
			}
			catch (IOException exc)
			{
				throw new UncheckedIOException(exc);
			}
		}
	}
	
	
	
	
	
	@Override
	public Object get(int columnIndex) throws IllegalStateException
	{
		if (isBOF())
			throw new IllegalStateException("BOF");
		
		if (isEOF())
			throw new IllegalStateException("EOF");
		
		return currentRowData[columnIndex];
	}
	
	
	
	
	@Override
	public void close()
	{
		try
		{
			lowlevel.close();
		}
		catch (IOException exc)
		{
			throw new UncheckedIOException(exc);
		}
	}
}
