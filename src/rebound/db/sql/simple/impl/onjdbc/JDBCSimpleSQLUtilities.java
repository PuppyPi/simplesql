package rebound.db.sql.simple.impl.onjdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JDBCSimpleSQLUtilities
{
	public static void setPreparedStatementParameters(PreparedStatement s, Object[] parameterValues) throws SQLException
	{
		int n = parameterValues.length;
		
		for (int i = 0; i < n; i++)
			s.setObject(i+1, parameterValues[i]);
	}
}
