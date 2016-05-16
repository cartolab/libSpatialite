package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import static es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.SpatiaLiteDriver.NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hardcode.gdbms.driver.exceptions.InitializeWriterException;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.db.utils.ConnectionWithParams;
import com.iver.cit.gvsig.fmap.drivers.db.utils.SingleVectorialDBConnectionManager;
import com.iver.cit.gvsig.fmap.layers.LayerFactory;

import es.icarto.gvsig.commons.testutils.Drivers;

public class CreateTableTest {

    private String dbpath;
    private SpatiaLiteDriver driver;
    private ConnectionJDBC conJbdc;

    @BeforeClass
    public static void setUpBeforeClass() {
	NativeDependencies.libsPath = TestUtils.libPath;

	// TODO
	Drivers.initgvSIGDrivers("../_fwAndami/gvSIG/extensiones/com.iver.cit.gvsig/drivers");
    }

    @Before
    public void setUp() throws Exception {
	File db = File.createTempFile("test", ".sqlite");
	db.deleteOnExit();
	dbpath = db.getAbsolutePath();
	SingleVectorialDBConnectionManager ins = SingleVectorialDBConnectionManager
		.instance();
	ConnectionWithParams conwp = ins.getConnection(NAME, "", "",
		"spatialite", dbpath, "", "", true);

	driver = (SpatiaLiteDriver) LayerFactory.getDM().getDriver(NAME);
	driver.setConnection(conwp.getConnection());

	conJbdc = ((ConnectionJDBC) conwp.getConnection());
	initSpatialData();
    }

    @Test
    public void testCreateTable() throws InitializeWriterException,
	    SQLException {

	SpatiaLiteWriter writer = new SpatiaLiteWriter();
	writer.setCreateTable(true);
	DBLayerDefinition lyrD = TestUtils.getLyrDef();
	lyrD.setConnection(conJbdc);
	writer.initialize(lyrD);
	assertTrue(existTable(TestUtils.testTableName));
    }

    @Test
    // TODO. Create table should provide a method to automatically create the
    // index
    public void testCreateTableWithSpatialIndex()
	    throws InitializeWriterException, SQLException {

	SpatiaLiteWriter writer = new SpatiaLiteWriter();
	writer.setCreateTable(true);
	DBLayerDefinition lyrD = TestUtils.getLyrDef();
	lyrD.setConnection(conJbdc);
	writer.initialize(lyrD);
	SpatiaLite spatialite = new SpatiaLite();
	String createIndex = spatialite.getSqlCreateIndex(lyrD);
	Statement st = conJbdc.getConnection().createStatement();
	st.executeQuery(createIndex);
	conJbdc.getConnection().commit();

	ResultSet rs = st
		.executeQuery("SELECT CheckSpatialIndex('test','geom')");
	rs.next();
	assertEquals(1, rs.getInt(1));

    }

    /**
     * TODO Seguramente esto debería moverse a cuando se establece la conexión o
     * a cuando se setea la conexión en el driver Otra opción sería en lugar de
     * usar jdbc:sqlite usar nuestro propio identificador de modo que cargar las
     * nativas, y similares se hiciera de forma más controlada
     *
     * http://www.gaia-gis.it/gaia-sins/spatialite-cookbook/html/metadata.html
     */
    private void initSpatialData() throws SQLException {
	Statement st = conJbdc.getConnection().createStatement();
	st.executeQuery("SELECT InitSpatialMetaData()");
	conJbdc.getConnection().commit();
	st.close();
    }

    private boolean existTable(String tableName) throws SQLException {
	boolean exists = false;

	String sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?;";

	PreparedStatement st = conJbdc.getConnection().prepareStatement(sql);
	st.setString(1, tableName);
	ResultSet rs = st.executeQuery();
	if (rs.next()) {
	    exists = true;
	}
	rs.close();
	st.close();

	return exists;
    }
}
