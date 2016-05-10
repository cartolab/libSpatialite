package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.sqlite.core.CoreConnection;

import com.hardcode.gdbms.driver.exceptions.InitializeWriterException;
import com.hardcode.gdbms.driver.exceptions.ReadDriverException;
import com.hardcode.gdbms.driver.exceptions.ReloadDriverException;
import com.hardcode.gdbms.engine.instruction.IncompatibleTypesException;
import com.hardcode.gdbms.engine.values.BooleanValue;
import com.hardcode.gdbms.engine.values.Value;
import com.iver.cit.gvsig.exceptions.visitors.ProcessWriterVisitorException;
import com.iver.cit.gvsig.exceptions.visitors.StartWriterVisitorException;
import com.iver.cit.gvsig.exceptions.visitors.StopWriterVisitorException;
import com.iver.cit.gvsig.fmap.core.DefaultFeature;
import com.iver.cit.gvsig.fmap.core.IFeature;
import com.iver.cit.gvsig.fmap.drivers.ConnectionFactory;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.IFeatureIterator;
import com.iver.cit.gvsig.fmap.edition.IRowEdited;

import es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.SpatiaLiteDriver;
import es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.SpatiaLiteWriter;

public class CRUDTests {

    private static SpatiaLiteDriver driver = new SpatiaLiteDriver(true,
	    TestUtils.libPath);
    private SpatiaLiteWriter writer = new SpatiaLiteWriter();
    private static DBLayerDefinition testLyrDef = TestUtils.getLyrDef();
    private ConnectionJDBC conn;

    /**
     * We manually create some rows for testing purposes
     */
    @BeforeClass
    public static void createTestRows() throws DBException,
    InitializeWriterException, SQLException {
	File file = new File(TestUtils.testDataPath);
	String connectionString = driver.getConnectionString(
		file.getAbsolutePath(), "", "", "", "");
	ConnectionJDBC conn = (ConnectionJDBC) ConnectionFactory
		.createConnection(connectionString, "", "");
	testLyrDef.setConnection(conn);
	driver.setData(conn, testLyrDef);
	conn.getConnection().setAutoCommit(true);
	conn.getConnection().createStatement()
	.execute(TestUtils.readRowInsertSql);
	conn.getConnection().createStatement()
	.execute(TestUtils.deleteRowInsertSql);
	conn.getConnection().createStatement()
	.execute(TestUtils.updateRowInsertSql);
	((CoreConnection) conn.getConnection()).realClose();
    }

    /**
     * We delete all rows
     */
    @AfterClass
    public static void deleteTestRows() throws DBException,
    InitializeWriterException, SQLException {
	File file = new File(TestUtils.testDataPath);
	String connectionString = driver.getConnectionString(
		file.getAbsolutePath(), "", "", "", "");
	ConnectionJDBC conn = (ConnectionJDBC) ConnectionFactory
		.createConnection(connectionString, "", "");
	testLyrDef.setConnection(conn);
	driver.setData(conn, testLyrDef);
	conn.getConnection().createStatement()
	.execute(TestUtils.deleteAllRowsSql);
	conn.getConnection().setAutoCommit(true);
	((CoreConnection) conn.getConnection()).realClose();
    }

    @Before
    public void prepareDriver() throws DBException, InitializeWriterException,
    SQLException, ReloadDriverException {
	File file = new File(TestUtils.testDataPath);
	String connectionString = driver.getConnectionString(
		file.getAbsolutePath(), "", "", "", "");
	conn = (ConnectionJDBC) ConnectionFactory.createConnection(
		connectionString, "", "");
	conn.getConnection().setAutoCommit(false);

	testLyrDef.setConnection(conn);
	driver.setData(conn, testLyrDef);
	writer.initialize(testLyrDef);
	driver.reload();
    }

    @After
    public void closeConnection() throws SQLException {
	((CoreConnection) conn.getConnection()).realClose();
    }

    private void fullProcess(IRowEdited row)
	    throws StartWriterVisitorException, ProcessWriterVisitorException,
	    StopWriterVisitorException {
	writer.preProcess();
	writer.process(row);
	writer.postProcess();
    }

    /**
     * We try retrieving one of the rows we created in the startup method
     */
    @Test
    public void readRow() throws ReadDriverException,
    IncompatibleTypesException, IOException {
	IRowEdited row = TestUtils.readRow;
	Value[] values = row.getAttributes();
	IFeatureIterator features = driver.getFeatureIterator(driver
		.getSqlTotal());
	boolean found = false;
	// We look for the specific row
	while (features.hasNext()) {
	    IFeature feature = features.next();
	    if (((BooleanValue) values[0].equals(feature.getAttribute(0)))
		    .getValue()) {
		found = true;
		// We check that the row has all the expected values
		assertEquals(values[1], feature.getAttribute(1));
		assertEquals(values[2], feature.getAttribute(2));
		assertEquals(values[3], feature.getAttribute(3));
		assertEquals(values[4], feature.getAttribute(4));
		assertArrayEquals(((DefaultFeature) row.getLinkedRow())
			.getGeometry().toWKB(), feature.getGeometry().toWKB());
		break;
	    }
	}
	// We check that we actually found the row
	assertTrue(found);
    }

    /**
     * We try creating a new row
     */
    @Test
    public void createRow() throws ProcessWriterVisitorException,
    ReadDriverException, StartWriterVisitorException,
    StopWriterVisitorException, IncompatibleTypesException, IOException {
	long previousCount = driver.getRowCount();
	IRowEdited row = TestUtils.createRow;
	fullProcess(row);
	driver.reload();
	assertEquals(previousCount + 1, driver.getRowCount());
	Value[] values = row.getAttributes();
	IFeatureIterator features = driver.getFeatureIterator(driver
		.getSqlTotal());
	boolean found = false;
	// We look for the specific row
	while (features.hasNext()) {
	    IFeature feature = features.next();
	    if (((BooleanValue) values[0].equals(feature.getAttribute(0)))
		    .getValue()) {
		found = true;
		// We check that the row has all the expected values
		assertEquals(values[1], feature.getAttribute(1));
		assertEquals(values[2], feature.getAttribute(2));
		assertEquals(values[3], feature.getAttribute(3));
		assertEquals(values[4], feature.getAttribute(4));
		assertArrayEquals(((DefaultFeature) row.getLinkedRow())
			.getGeometry().toWKB(), feature.getGeometry().toWKB());
		break;
	    }
	}
	// We check that we actually found the row
	assertTrue(found);
    }

    /**
     * We try deleting one of the rows we created in the startup method
     */
    @Test
    public void deleteRow() throws ProcessWriterVisitorException,
    StartWriterVisitorException, StopWriterVisitorException,
    ReadDriverException, IncompatibleTypesException {
	long previousCount = driver.getRowCount();
	IRowEdited row = TestUtils.deleteRow;
	fullProcess(row);
	driver.reload();
	assertEquals(previousCount - 1, driver.getRowCount());
	Value[] values = row.getAttributes();
	IFeatureIterator features = driver.getFeatureIterator(driver
		.getSqlTotal());
	boolean found = false;
	// We look for the specific row
	while (features.hasNext()) {
	    IFeature feature = features.next();
	    if (((BooleanValue) values[0].equals(feature.getAttribute(0)))
		    .getValue()) {
		found = true;
	    }
	}
	// We check that the row doesn't any longer exist
	assertTrue(!found);
    }

    /**
     * We try updating one of the rows we created in the startup method
     */
    @Test
    public void updateRow() throws ProcessWriterVisitorException,
    StartWriterVisitorException, StopWriterVisitorException,
    ReadDriverException, IncompatibleTypesException, IOException {
	long previousCount = driver.getRowCount();
	IRowEdited row = TestUtils.updateRow;
	fullProcess(row);
	driver.reload();
	assertEquals(previousCount, driver.getRowCount());
	Value[] values = row.getAttributes();
	IFeatureIterator features = driver.getFeatureIterator(driver
		.getSqlTotal());
	boolean found = false;
	while (features.hasNext()) {
	    IFeature feature = features.next();
	    if (((BooleanValue) values[0].equals(feature.getAttribute(0)))
		    .getValue()) {
		found = true;
		// We check that the row has all the expected values
		assertEquals(values[1], feature.getAttribute(1));
		assertEquals(values[2], feature.getAttribute(2));
		assertEquals(values[3], feature.getAttribute(3));
		assertEquals(values[4], feature.getAttribute(4));
		assertArrayEquals(((DefaultFeature) row.getLinkedRow())
			.getGeometry().toWKB(), feature.getGeometry().toWKB());
		break;
	    }
	}
	// We check that we actually found the row
	assertTrue(found);
    }

}
