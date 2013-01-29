package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import java.util.HashSet;
import java.util.Set;

public abstract class SQLiteReservedWords {
	private static Set<String> reserverdWords = new HashSet<String>();
	
	static {
		reserverdWords.add("ABORT");
		reserverdWords.add("ACTION");
		reserverdWords.add("ADD");
		reserverdWords.add("AFTER");
		reserverdWords.add("ALL");
		reserverdWords.add("ALTER");
		reserverdWords.add("ANALYZE");
		reserverdWords.add("AND");
		reserverdWords.add("AS");
		reserverdWords.add("ASC");
		reserverdWords.add("ATTACH");
		reserverdWords.add("AUTOINCREMENT");
		reserverdWords.add("BEFORE");
		reserverdWords.add("BEGIN");
		reserverdWords.add("BETWEEN");
		reserverdWords.add("BY");
		reserverdWords.add("CASCADE");
		reserverdWords.add("CASE");
		reserverdWords.add("CAST");
		reserverdWords.add("CHECK");
		reserverdWords.add("COLLATE");
		reserverdWords.add("COLUMN");
		reserverdWords.add("COMMIT");
		reserverdWords.add("CONFLICT");
		reserverdWords.add("CONSTRAINT");
		reserverdWords.add("CREATE");
		reserverdWords.add("CROSS");
		reserverdWords.add("CURRENT_DATE");
		reserverdWords.add("CURRENT_TIME");
		reserverdWords.add("CURRENT_TIMESTAMP");
		reserverdWords.add("DATABASE");
		reserverdWords.add("DEFAULT");
		reserverdWords.add("DEFERRABLE");
		reserverdWords.add("DEFERRED");
		reserverdWords.add("DELETE");
		reserverdWords.add("DESC");
		reserverdWords.add("DETACH");
		reserverdWords.add("DISTINCT");
		reserverdWords.add("DROP");
		reserverdWords.add("EACH");
		reserverdWords.add("ELSE");
		reserverdWords.add("END");
		reserverdWords.add("ESCAPE");
		reserverdWords.add("EXCEPT");
		reserverdWords.add("EXCLUSIVE");
		reserverdWords.add("EXISTS");
		reserverdWords.add("EXPLAIN");
		reserverdWords.add("FAIL");
		reserverdWords.add("FOR");
		reserverdWords.add("FOREIGN");
		reserverdWords.add("FROM");
		reserverdWords.add("FULL");
		reserverdWords.add("GLOB");
		reserverdWords.add("GROUP");
		reserverdWords.add("HAVING");
		reserverdWords.add("IF");
		reserverdWords.add("IGNORE");
		reserverdWords.add("IMMEDIATE");
		reserverdWords.add("IN");
		reserverdWords.add("INDEX");
		reserverdWords.add("INDEXED");
		reserverdWords.add("INITIALLY");
		reserverdWords.add("INNER");
		reserverdWords.add("INSERT");
		reserverdWords.add("INSTEAD");
		reserverdWords.add("INTERSECT");
		reserverdWords.add("INTO");
		reserverdWords.add("IS");
		reserverdWords.add("ISNULL");
		reserverdWords.add("JOIN");
		reserverdWords.add("KEY");
		reserverdWords.add("LEFT");
		reserverdWords.add("LIKE");
		reserverdWords.add("LIMIT");
		reserverdWords.add("MATCH");
		reserverdWords.add("NATURAL"); 
		reserverdWords.add("NCHAR");
		reserverdWords.add("NO");
		reserverdWords.add("NOT");
		reserverdWords.add("NOTNULL");
		reserverdWords.add("NULL");
		reserverdWords.add("OF");
		reserverdWords.add("OFFSET");
		reserverdWords.add("ON");
		reserverdWords.add("OR");
		reserverdWords.add("ORDER");
		reserverdWords.add("OUTER");
		reserverdWords.add("PLAN");
		reserverdWords.add("PRAGMA");
		reserverdWords.add("PRIMARY");
		reserverdWords.add("QUERY");
		reserverdWords.add("RAISE");
		reserverdWords.add("REFERENCES");
		reserverdWords.add("REGEXP");
		reserverdWords.add("REINDEX");
		reserverdWords.add("RELEASE");
		reserverdWords.add("RENAME");
		reserverdWords.add("REPLACE");
		reserverdWords.add("RESTRICT");
		reserverdWords.add("RIGHT");
		reserverdWords.add("ROLLBACK");
		reserverdWords.add("ROW");
		reserverdWords.add("SAVEPOINT");
		reserverdWords.add("SELECT");
		reserverdWords.add("SET");
		reserverdWords.add("TABLE");
		reserverdWords.add("TEMP");
		reserverdWords.add("TEMPORARY");
		reserverdWords.add("THEN");
		reserverdWords.add("TO");
		reserverdWords.add("TRANSACTION");
		reserverdWords.add("TRIGGER");
		reserverdWords.add("UNION");
		reserverdWords.add("UNIQUE");
		reserverdWords.add("UPDATE");
		reserverdWords.add("USING");
		reserverdWords.add("VACUUM");
		reserverdWords.add("VALUES");
		reserverdWords.add("VIEW");
		reserverdWords.add("VIRTUAL");
		reserverdWords.add("WHEN");
		reserverdWords.add("WHERE");
	}
	
	public static boolean isReserved(String word){
		return reserverdWords.contains(word.toUpperCase());
	}
}
