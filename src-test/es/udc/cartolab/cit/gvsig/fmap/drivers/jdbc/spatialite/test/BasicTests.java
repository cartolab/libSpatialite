package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
//import org.sqlite.Conn;
import org.sqlite.SQLiteConnection;
import org.sqlite.core.CoreConnection;

import com.hardcode.gdbms.driver.exceptions.ReadDriverException;
import com.iver.cit.gvsig.fmap.drivers.ConnectionFactory;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;

import es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.SpatiaLiteDriver;

public class BasicTests {

	private static SpatiaLiteDriver driver = new SpatiaLiteDriver(true,
			TestUtils.libPath);
	private ConnectionJDBC conn;
	private static DBLayerDefinition testLyrDef = TestUtils.getLyrDef();

	@Before
	public void createConnection() throws DBException {
		File file = new File(TestUtils.testDataPath);
		String connectionString = driver.getConnectionString(
				file.getAbsolutePath(), "", "", "", "");

		conn = (ConnectionJDBC) ConnectionFactory.createConnection(
				connectionString, "", "");

		testLyrDef.setConnection(conn);
		driver.setData(conn, testLyrDef);
	}

	@After
	public void closeConnection() throws SQLException {
		((CoreConnection) conn.getConnection()).realClose();
	}

	/**
	 * Method for testing the metadata retrieval from the whole database
	 */
	@Test
	public void listTablesTest() throws DBException {
		Set<String> tables = new HashSet<String>(Arrays.asList(driver
				.getTableNames(conn, null)));
		Set<String> dbTables = new HashSet<String>(
				Arrays.asList(TestUtils.baseTables));
		dbTables.addAll(Arrays.asList(TestUtils.testTables));
		// We check the retrieved tables contain all the expected ones
		assertTrue(tables.containsAll(dbTables));
	}

	/**
	 * Method for testing the metadata retrieval from an empty table
	 */
	@Test
	public void readVoidLayer() throws ReadDriverException {
		// We check the table definition contains all the expected fields
		assertArrayEquals(TestUtils.testTableAlphFields, driver.getFields());
		// We check the table has no rows
		assertEquals(0, driver.getRowCount());
	}

}
