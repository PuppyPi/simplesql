package rebound.db.sql.simple.impl.snapshotresults.custombinary;

import static rebound.io.util.BasicIOUtilities.*;
import static rebound.util.collections.BasicCollectionUtilities.*;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import rebound.annotations.semantic.allowedoperations.ReadonlyValue;
import rebound.db.sql.simple.api.SimpleSQLRandomAccessResults;
import rebound.db.sql.simple.api.SimpleSQLResults;
import rebound.io.iio.OutputByteStream;
import rebound.io.iio.sio.RandomAccessFileWrapper;
import rebound.io.iio.unions.CloseableFlushableRandomAccessBytesInterface;
import rebound.util.collections.PairOrdered;
import rebound.util.functional.throwing.FunctionalInterfacesThrowingCheckedExceptionsStandard.NullaryFunctionThrowingIOException;

public class CustomBinarySnapshotUtilities
{
	public static final NullaryFunctionThrowingIOException<File> DefaultTemporaryFileMaker = () -> File.createTempFile("simplesql-snapshot-buffer", ".ssqldata");
	
	
	public static SimpleSQLRandomAccessResults performLocallyBufferedSnapshotting(SimpleSQLResults underlying)
	{
		return performLocallyBufferedSnapshotting(underlying, DefaultTemporaryFileMaker);
	}
	
	public static SimpleSQLRandomAccessResults performLocallyBufferedSnapshotting(SimpleSQLResults underlying, NullaryFunctionThrowingIOException<File> temporaryFileMaker)
	{
		File tempFile;
		try
		{
			tempFile = temporaryFileMaker.f();
		}
		catch (IOException exc)
		{
			throw new UncheckedIOException(exc);
		}
		
		boolean success = false;
		try
		{
			tempFile.deleteOnExit();
			
			//Todo why don't we use the buffering wrapper??
			//CloseableFlushableRandomAccessBytesInterface lowlevel = new RandomAccessBytesBufferingWrapper(new RandomAccessFileWrapper(tempFile, true));
			CloseableFlushableRandomAccessBytesInterface lowlevel = new RandomAccessFileWrapper(tempFile, true);
			
			try
			{
				long numberOfRows;
				CustomSimpleSQLSupportedTypes[] columnDatatypes;
				{
					PairOrdered<Long, CustomSimpleSQLSupportedTypes[]> r = writeEntiretyFromSimpleSQL(lowlevel, underlying);
					numberOfRows = r.getA();
					columnDatatypes = r.getB();  //may be null but that's okay :3
				}
				
				if (numberOfRows == 0)
				{
					closeWithoutError(lowlevel);
					tempFile.delete();
				}
				else
				{
					//Return to the start for reading!
					lowlevel.seekToStart();
				}
				
				@Nullable Runnable onClose = numberOfRows == 0 ? null : () ->
				{
					closeWithoutError(lowlevel);
					tempFile.delete();
				};
				
				
				CustomBinarySnapshotSimpleSQLResultsReader r = new CustomBinarySnapshotSimpleSQLResultsReader(numberOfRows, lowlevel, columnDatatypes, onClose);
				success = true;
				return r;
			}
			finally
			{
				if (!success)
					lowlevel.close();
			}
		}
		catch (IOException exc)
		{
			throw new UncheckedIOException(exc);
		}
		finally
		{
			if (!success)
				tempFile.delete();
		}
	}
	
	
	
	
	
	
	
	
	
	
	
	/**
	 * @return pair(0, null) for empty data with no rows!
	 */
	public static @Nonnull PairOrdered<Long, CustomSimpleSQLSupportedTypes[]> writeEntiretyFromSimpleSQL(@ReadonlyValue OutputByteStream lowlevel, SimpleSQLResults input) throws IOException
	{
		if (input.next())
		{
			int columnCount = input.getColumnCount();
			
			CustomSimpleSQLSupportedTypes[] columnDatatypes = new CustomSimpleSQLSupportedTypes[columnCount];
			Arrays.fill(columnDatatypes, CustomSimpleSQLSupportedTypes.WasNullForAllRows);
			
			Object[] rowData = new Object[columnCount];
			byte[] nullFlags = new byte[CustomBinarySnapshotFormat.sizeOfNullFlagsArray(columnCount)];
			
			long numberOfRows = 1;
			
			while (true)
			{
				for (int i = 0; i < columnCount; i++)
				{
					Object v = input.get(i);
					
					if (columnDatatypes[i] == CustomSimpleSQLSupportedTypes.WasNullForAllRows && v != null)
						columnDatatypes[i] = CustomBinarySnapshotFormat.getTypeFromObject(v);
					
					//If v != null and there is a datatype already, writeRow() will validate it's the right type :3
					
					rowData[i] = v;
				}
				
				CustomBinarySnapshotFormat.writeRow(lowlevel, columnDatatypes, nullFlags, rowData);
				
				if (input.next())
					numberOfRows++;
				else
					break;
			}
			
			return pair(numberOfRows, columnDatatypes);
		}
		else
		{
			//Empty data!
			return pair(0l, null);
		}
	}
}
