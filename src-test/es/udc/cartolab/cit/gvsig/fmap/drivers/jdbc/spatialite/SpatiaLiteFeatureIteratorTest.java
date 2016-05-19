package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import static es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite.SpatiaLiteDriver.NAME;
import static org.junit.Assert.assertEquals;

import java.awt.geom.Rectangle2D;
import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.cresques.cts.IProjection;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.hardcode.gdbms.engine.values.IntValue;
import com.iver.cit.gvsig.fmap.MapContext;
import com.iver.cit.gvsig.fmap.ViewPort;
import com.iver.cit.gvsig.fmap.core.IFeature;
import com.iver.cit.gvsig.fmap.core.IGeometry;
import com.iver.cit.gvsig.fmap.crs.CRSFactory;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.IFeatureIterator;
import com.iver.cit.gvsig.fmap.drivers.WKBParser3;
import com.iver.cit.gvsig.fmap.drivers.db.utils.ConnectionWithParams;
import com.iver.cit.gvsig.fmap.drivers.db.utils.SingleVectorialDBConnectionManager;
import com.iver.cit.gvsig.fmap.layers.FLayers;
import com.iver.cit.gvsig.fmap.layers.FLyrVect;
import com.iver.cit.gvsig.fmap.layers.LayerFactory;
import com.iver.cit.gvsig.fmap.layers.SelectableDataSource;

import es.icarto.gvsig.commons.testutils.Drivers;

public class SpatiaLiteFeatureIteratorTest {

    private static final IProjection EPSG = CRSFactory.getCRS("EPSG:32616");
    private String dbpath;
    private SpatiaLiteDriver driver;
    private ConnectionJDBC conJbdc;
    private DBLayerDefinition lyrD;
    private static final int nrows = 2 * SpatiaLiteFeatureIterator.FETCH_SIZE + 1;

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
	lyrD = TestUtils.getLyrDef();
	lyrD.setConnection(conJbdc);
	initSpatialData();
	createTableWithSpatialIndex();
	TestUtils.populateTable(conJbdc.getConnection(), nrows);
    }

    @After
    public void tearDown() throws Exception {
	SingleVectorialDBConnectionManager.instance().closeAllBeforeTerminate();
    }

    /**
     * Los puntos principales donde se crea/llama un iterador son:
     *
     * <pre>
     * DBStrategy
     *  + draw <- FLyVect.draw. No pasa nunca.
     *  + process <- Geoprocesos fundamentalmente. Poco importante
     *  + queryByRect  <- FLyVect queryByRect y queryByPoint
     *
     * FLyrVect
     *  + _draw <- FLyVect.draw cuando la strategy es null
     *  + _print <- .. <- FFrameView.draw
     *  + getFullExtent
     * </pre>
     *
     * @throws Exception
     */
    @Test
    public void testNumberOfRows() throws Exception {
	FLyrVect layer = getLayer();

	assertEquals(nrows, driver.getRowCount());
	assertEquals(nrows, layer.getRecordset().getRowCount());
	assertEquals(nrows, layer.getSource().getShapeCount());
    }

    @Test
    public void testFullExtent() throws Exception {

	FLyrVect layer = getLayer();

	Rectangle2D fullExtent = layer.getFullExtent();
	Statement st = conJbdc.getConnection().createStatement();
	ResultSet rs = st.executeQuery("SELECT ST_AsBinary(extent(geom)) from "
		+ TestUtils.testTableName);
	rs.next();
	IGeometry parse = new WKBParser3().parse(rs.getBytes(1));
	assertEquals(parse.getBounds2D(), fullExtent);

    }

    @Test
    public void testDraw() throws Exception {
	FLyrVect layer = getLayer();
	// layer.draw(image, g, viewPort, cancel, scale);
    }

    @Test
    public void testReadWithDriverIterator() throws Exception {
	FLyrVect layer = getLayer();
	SelectableDataSource rs = layer.getRecordset();

	String sql = "SELECT AsBinary(geom), gid, entero from "
		+ TestUtils.testTableName;
	IFeatureIterator it = driver.getFeatureIterator(sql);
	int value = 0;
	while (it.hasNext()) {
	    IFeature feat = it.next();
	    IntValue intValue = (IntValue) feat.getAttribute(2);
	    assertEquals(value++, intValue.intValue());
	}
	it.closeIterator();
    }

    @Test
    public void testReadDirectlyOneFetch() throws Exception {
	FLyrVect layer = getLayer();
	SelectableDataSource rs = layer.getRecordset();

	IntValue fieldValue = (IntValue) rs.getFieldValue(5, 2);
	assertEquals(5, fieldValue.intValue());

	fieldValue = (IntValue) rs.getFieldValue(10, 2);
	assertEquals(10, fieldValue.intValue());

	fieldValue = (IntValue) rs.getFieldValue(1, 2);
	assertEquals(1, fieldValue.intValue());

	fieldValue = (IntValue) rs.getFieldValue(0, 2);
	assertEquals(0, fieldValue.intValue());

	fieldValue = (IntValue) rs.getFieldValue(15, 2);
	assertEquals(15, fieldValue.intValue());
    }

    public void testReadDirectlySeveralFetch() throws Exception {
	FLyrVect layer = getLayer();
	SelectableDataSource rs = layer.getRecordset();

	IntValue fieldValue = (IntValue) rs.getFieldValue(5000, 2);
	assertEquals(5000, fieldValue.intValue());

	fieldValue = (IntValue) rs.getFieldValue(0, 2);
	assertEquals(0, fieldValue.intValue());

	fieldValue = (IntValue) rs.getFieldValue(15, 2);
	assertEquals(15, fieldValue.intValue());

	fieldValue = (IntValue) rs.getFieldValue(10000, 2);
	assertEquals(10000, fieldValue.intValue());
    }

    private FLyrVect getLayer() throws Exception {
	final DBLayerDefinition lyrDef = TestUtils.getLyrDef();
	driver.setData(conJbdc, lyrDef);
	driver.reload();

	FLyrVect lyr = (FLyrVect) LayerFactory.createDBLayer(driver,
		"testLayer", EPSG);
	FLayers flayers = new FLayers();
	ViewPort viewPort = new ViewPort(EPSG);
	MapContext mapContext = new MapContext(viewPort);
	flayers.setMapContext(mapContext);
	lyr.setParentLayer(flayers);

	return lyr;
    }

    private void initSpatialData() throws SQLException {
	Statement st = conJbdc.getConnection().createStatement();
	st.executeQuery("SELECT InitSpatialMetaData()");
	conJbdc.getConnection().commit();
	st.close();
    }

    public void createTableWithSpatialIndex() throws Exception {
	SpatiaLiteWriter writer = new SpatiaLiteWriter();
	writer.setCreateTable(true);
	writer.initialize(lyrD);
	SpatiaLite spatialite = new SpatiaLite();
	String createIndex = spatialite.getSqlCreateIndex(lyrD);
	Statement st = conJbdc.getConnection().createStatement();
	st.executeQuery(createIndex);
	conJbdc.getConnection().commit();
    }
}
