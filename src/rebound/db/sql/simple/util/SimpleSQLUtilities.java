package rebound.db.sql.simple.util;

import static rebound.bits.BitfieldSafeCasts.*;
import rebound.db.sql.simple.api.SimpleSQLResults;
import rebound.db.sql.simple.api.SimpleSQLResults.CurrentRecordView;
import rebound.exceptions.StopIterationReturnPath;
import rebound.exceptions.UnsupportedOptionException;

public class SimpleSQLUtilities
{
	public static int decodeCountQuery(SimpleSQLResults r)
	{
		CurrentRecordView row;
		try
		{
			row = r.nextrp();
		}
		catch (StopIterationReturnPath exc)
		{
			throw new UnsupportedOptionException("SQL COUNT() query returned zero rows!!");
		}
		
		int n = row.size();
		if (n != 1)
			throw new UnsupportedOptionException("SQL COUNT() query didn't return one column!!  It returned "+n+" columns!");
		
		int count = safeCastS64toS32(row.getAsLong(0));
		
		if (r.next())
			throw new UnsupportedOptionException("SQL COUNT() query returned more than one row!!");
		
		return count;
	}
}
