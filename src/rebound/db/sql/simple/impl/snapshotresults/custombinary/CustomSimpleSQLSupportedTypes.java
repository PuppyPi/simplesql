package rebound.db.sql.simple.impl.snapshotresults.custombinary;

import rebound.annotations.semantic.SemanticOrdinals;

@SemanticOrdinals
public enum CustomSimpleSQLSupportedTypes
{
	WasNullForAllRows,  //we couldn't figure out the type during writing it..but then again you don't need it, so no worries! XD :D
	
	Boolean,
	Byte,
	Short,
	Int,
	Long,
	Float,
	Double,
	
	ByteString,
	UnicodeString,
	UUID,
	;
	
	
	protected static final CustomSimpleSQLSupportedTypes[] a = CustomSimpleSQLSupportedTypes.class.getEnumConstants();
	
	public static CustomSimpleSQLSupportedTypes forOrdinal(int i) throws IndexOutOfBoundsException
	{
		return a[i];
	}
}
