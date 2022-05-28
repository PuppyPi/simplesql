package rebound.db.sql.simple.impl.snapshotresults.custombinary;

import static rebound.bits.BitUtilities.*;
import static rebound.bits.Bytes.*;
import static rebound.io.util.BasicIOUtilities.*;
import static rebound.math.SmallIntegerMathUtilities.*;
import static rebound.text.StringUtilities.*;
import static rebound.util.BasicExceptionUtilities.*;
import static rebound.util.objectutil.ObjectUtilities.*;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import javax.annotation.Nonnull;
import rebound.annotations.semantic.allowedoperations.ReadonlyValue;
import rebound.annotations.semantic.allowedoperations.WritableValue;
import rebound.exceptions.BinarySyntaxIOException;
import rebound.io.iio.InputByteStream;
import rebound.io.iio.OutputByteStream;
import rebound.util.collections.Slice;
import rebound.util.collections.prim.PrimitiveCollections.ByteList;

public class CustomBinarySnapshotFormat
{
	protected static final Charset UnicodeEncoding = StandardCharsets.UTF_8;
	
	
	public static int sizeOfNullFlagsArray(int columnCount)
	{
		return ceildiv(columnCount, 8);
	}
	
	
	
	/**
	 * @param nullFlags this method reads and writes this, you just need to make sure it's the right size with {@link #sizeOfNullFlagsArray(int)} :3
	 */
	public static void readRow(@ReadonlyValue InputByteStream lowlevel, @ReadonlyValue CustomSimpleSQLSupportedTypes[] columnDatatypes, @WritableValue byte[] nullFlags, @WritableValue Object[] rowData) throws IOException, EOFException, BinarySyntaxIOException
	{
		int columnCount = columnDatatypes.length;
		if (rowData.length != columnCount)
			throw new IllegalArgumentException();
		
		
		//Read nullFlags
		readFully(lowlevel, nullFlags);
		
		
		//Read packed data :3
		{
			for (int i = 0; i < columnCount; i++)
			{
				//Using an enum here instead of java.lang.Class's allows compilation to a near-jump lookup table in machine code..whether the JIT is smart enough to do that idk..but honestly it probably doesn't matter anyway because I'm sure it's all I/O bottlenecked anyway XD''
				//	(it's also nicer in case there's subclasses of things and the runtime type is ambiguous!)
				
				Object v;
				{
					if (getBit(nullFlags, i))  //0 = null, 1 = nonnull
					{
						CustomSimpleSQLSupportedTypes t = columnDatatypes[i];
						
						if (t == CustomSimpleSQLSupportedTypes.Boolean)
						{
							byte r = read1(lowlevel);
							
							if (r == 0)
								v = false;
							else if (r == 1)
								v = true;
							else
								throw BinarySyntaxIOException.inst(hexint(r));
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.Byte)
						{
							v = read1(lowlevel);
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.Short)
						{
							v = getLittleShort(lowlevel);
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.Int)
						{
							v = getLittleInt(lowlevel);
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.Long)
						{
							v = getLittleLong(lowlevel);
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.Float)
						{
							v = getLittleFloat(lowlevel);
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.Double)
						{
							v = getLittleDouble(lowlevel);
						}
						
						
						else if (t == CustomSimpleSQLSupportedTypes.UnicodeString)
						{
							int length = getLittleInt(lowlevel);
							byte[] raw = new byte[length];
							readFully(lowlevel, raw);
							v = decodeTextToStringReportingUnchecked(raw, UnicodeEncoding);
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.UUID)
						{
							long lsb = getLittleLong(lowlevel);  //little-endian
							long msb = getLittleLong(lowlevel);  //little-endian
							v = new UUID(msb, lsb);
						}
						
						else if (t == CustomSimpleSQLSupportedTypes.ByteString)
						{
							int length = getLittleInt(lowlevel);
							byte[] raw = new byte[length];
							readFully(lowlevel, raw);
							v = raw;
						}
						
						else
						{
							throw new IllegalArgumentException(toStringNT(t));
						}
					}
					else
					{
						v = null;
					}
				}
				
				
				rowData[i] = v;
			}
		}
	}
	
	
	/**
	 * @param columnDatatypes  this can contain nulls or {@link CustomSimpleSQLSupportedTypes#WasNullForAllRows} as long as the corresponding entry in <code>rowData</code> is null!
	 * @param nullFlags this method reads and writes this, you just need to make sure it's the right size with {@link #sizeOfNullFlagsArray(int)} :3
	 */
	public static void writeRow(@ReadonlyValue OutputByteStream lowlevel, @ReadonlyValue CustomSimpleSQLSupportedTypes[] columnDatatypes, @WritableValue byte[] nullFlags, @ReadonlyValue Object[] rowData) throws IOException
	{
		int columnCount = columnDatatypes.length;
		if (rowData.length != columnCount)
			throw new IllegalArgumentException();
		
		
		//Set nullFlags
		{
			for (int i = 0; i < columnCount; i++)
				setBit(nullFlags, i, rowData[i] != null);  //they're actually non-null flags X3
		}
		
		
		//Write nullFlags
		lowlevel.write(nullFlags);
		
		
		//Write packed data :3
		{
			for (int i = 0; i < columnCount; i++)
			{
				//Using an enum here instead of java.lang.Class's allows compilation to a near-jump lookup table in machine code..whether the JIT is smart enough to do that idk..but honestly it probably doesn't matter anyway because I'm sure it's all I/O bottlenecked anyway XD''
				//	(it's also nicer in case there's subclasses of things and the runtime type is ambiguous!)
				
				Object v = rowData[i];
				
				if (v != null)
				{
					CustomSimpleSQLSupportedTypes t = columnDatatypes[i];
					
					if (t == CustomSimpleSQLSupportedTypes.Boolean)
					{
						boolean vv = (Boolean) v;
						lowlevel.write(vv ? 1 : 0);
					}
					
					else if (t == CustomSimpleSQLSupportedTypes.Byte)
					{
						byte vv = (Byte) v;
						lowlevel.write(vv);
					}
					
					else if (t == CustomSimpleSQLSupportedTypes.Short)
					{
						short vv = (Short) v;
						putLittleShort(lowlevel, vv);
					}
					
					else if (t == CustomSimpleSQLSupportedTypes.Int)
					{
						int vv = (Integer) v;
						putLittleInt(lowlevel, vv);
					}
					
					else if (t == CustomSimpleSQLSupportedTypes.Long)
					{
						long vv = (Long) v;
						putLittleLong(lowlevel, vv);
					}
					
					else if (t == CustomSimpleSQLSupportedTypes.Float)
					{
						float vv = (Float) v;
						putLittleFloat(lowlevel, vv);
					}
					
					else if (t == CustomSimpleSQLSupportedTypes.Double)
					{
						double vv = (Double) v;
						putLittleDouble(lowlevel, vv);
					}
					
					
					else if (t == CustomSimpleSQLSupportedTypes.UnicodeString)
					{
						String vv = (String) v;
						byte[] encoded = encodeTextToByteArrayReportingUnchecked(vv, UnicodeEncoding);
						putLittleInt(lowlevel, encoded.length);
						lowlevel.write(encoded);
					}
					
					else if (t == CustomSimpleSQLSupportedTypes.UUID)
					{
						UUID vv = (UUID) v;
						
						putLittleLong(lowlevel, vv.getLeastSignificantBits());  //little-endian
						putLittleLong(lowlevel, vv.getMostSignificantBits());  //little-endian
					}
					
					
					else if (t == CustomSimpleSQLSupportedTypes.ByteString)
					{
						if (v instanceof ByteList)
							v = ((ByteList)v).toByteArraySlicePossiblyLive();
						
						if (v instanceof byte[])
						{
							byte[] vv = (byte[]) v;
							putLittleInt(lowlevel, vv.length);
							lowlevel.write(vv);
						}
						else if (v instanceof Slice)
						{
							Slice vv = (Slice) v;
							byte[] u = (byte[]) vv.getUnderlying();
							putLittleInt(lowlevel, vv.getLength());
							lowlevel.write(u, vv.getOffset(), vv.getLength());
						}
					}
					
					else
					{
						throw new IllegalArgumentException(toStringNT(t));
					}
				}
			}
		}
	}
	
	
	
	
	
	public static CustomSimpleSQLSupportedTypes getTypeFromObject(@Nonnull Object v)
	{
		if (v instanceof Byte)  return CustomSimpleSQLSupportedTypes.Byte;
		else if (v instanceof Short)  return CustomSimpleSQLSupportedTypes.Short;
		else if (v instanceof Integer)  return CustomSimpleSQLSupportedTypes.Int;
		else if (v instanceof Long)  return CustomSimpleSQLSupportedTypes.Long;
		else if (v instanceof Float)  return CustomSimpleSQLSupportedTypes.Float;
		else if (v instanceof Double)  return CustomSimpleSQLSupportedTypes.Double;
		else if (v instanceof Boolean)  return CustomSimpleSQLSupportedTypes.Boolean;
		else if (v instanceof String)  return CustomSimpleSQLSupportedTypes.UnicodeString;
		else if (v instanceof UUID)  return CustomSimpleSQLSupportedTypes.UUID;
		else if (v instanceof byte[] || v instanceof ByteList || (v instanceof Slice && ((Slice)v).getUnderlying() instanceof byte[]))  return CustomSimpleSQLSupportedTypes.ByteString;
		else  throw newClassCastExceptionOrNullPointerException(v);
	}
}
