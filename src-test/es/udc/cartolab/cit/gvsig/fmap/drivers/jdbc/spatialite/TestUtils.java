package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import com.hardcode.gdbms.engine.values.Value;
import com.hardcode.gdbms.engine.values.ValueFactory;
import com.iver.cit.gvsig.fmap.core.DefaultFeature;
import com.iver.cit.gvsig.fmap.core.FShape;
import com.iver.cit.gvsig.fmap.core.IGeometry;
import com.iver.cit.gvsig.fmap.core.ShapeFactory;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.FieldDescription;
import com.iver.cit.gvsig.fmap.edition.DefaultRowEdited;
import com.iver.cit.gvsig.fmap.edition.IRowEdited;

public abstract class TestUtils {

    public static String libPath = "native/lin32/";
    public static String testDataPath = "test-data" + File.separator
	    + "test.sqlite";
    public static String[] baseTables = { "SpatialIndex", "geom_cols_ref_sys",
	    "geometry_columns", "geometry_columns_auth", "spatial_ref_sys",
	    "spatialite_history", "sqlite_sequence", "views_geometry_columns",
	    "virts_geometry_columns" };
    public static String[] testTables = { "test" };
    public static String testTableName = "test";
    public static String testTableGeomField = "geom";
    public static String[] testTableAlphFields = { "gid", "texto", "entero",
	    "decimal", "bool" };
    public static String testTableIdField = "gid";
    public static String testTableEpsg = "EPSG:32616";
    public static int testTableRows = 0;
    public static IRowEdited readRow, createRow, deleteRow, updateRow;
    public static String readRowInsertSql = "INSERT INTO test(gid, texto, entero, decimal, bool, geom) "
	    + "VALUES (1, 'texto1', 101, 5.5, 0, GeomFromEWKT('SRID=32616;POINT(0 0)'));";
    public static String deleteRowInsertSql = "INSERT INTO test(gid, texto, entero, decimal, bool, geom) "
	    + "VALUES (2, 'texto2', 102, 6.5, 1, GeomFromEWKT('SRID=32616;POINT(1 1)'));";
    public static String updateRowInsertSql = "INSERT INTO test(gid, texto, entero, decimal, bool, geom) "
	    + "VALUES (3, 'texto3', 103, 7.5, 0, GeomFromEWKT('SRID=32616;POINT(2 2)'));";
    public static String deleteAllRowsSql = "DELETE FROM test;";

    static {
	Value[] rowValues = new Value[5];
	rowValues[0] = ValueFactory.createValue(4);
	rowValues[1] = ValueFactory.createValue("test");
	rowValues[2] = ValueFactory.createValue(10);
	rowValues[3] = ValueFactory.createValue(1.5);
	rowValues[4] = ValueFactory.createValue(true);
	IGeometry rowGeom = ShapeFactory.createPoint2D(45000, 5000);
	createRow = new DefaultRowEdited(new DefaultFeature(rowGeom, rowValues,
		rowValues[0].toString()), IRowEdited.STATUS_ADDED, 0);
	rowValues = new Value[5];
	rowValues[0] = ValueFactory.createValue(1);
	rowValues[1] = ValueFactory.createValue("texto1");
	rowValues[2] = ValueFactory.createValue(101);
	rowValues[3] = ValueFactory.createValue(5.5);
	rowValues[4] = ValueFactory.createValue(false);
	rowGeom = ShapeFactory.createPoint2D(0, 0);
	readRow = new DefaultRowEdited(new DefaultFeature(rowGeom, rowValues,
		rowValues[0].toString()), IRowEdited.STATUS_ORIGINAL, 0);
	rowValues = new Value[5];
	rowValues[0] = ValueFactory.createValue(2);
	rowValues[1] = ValueFactory.createValue("texto2");
	rowValues[2] = ValueFactory.createValue(102);
	rowValues[3] = ValueFactory.createValue(6.5);
	rowValues[4] = ValueFactory.createValue(true);
	rowGeom = ShapeFactory.createPoint2D(1, 1);
	deleteRow = new DefaultRowEdited(new DefaultFeature(rowGeom, rowValues,
		rowValues[0].toString()), IRowEdited.STATUS_DELETED, 0);
	rowValues = new Value[5];
	rowValues[0] = ValueFactory.createValue(3);
	rowValues[1] = ValueFactory.createValue("texto4");
	rowValues[2] = ValueFactory.createValue(105);
	rowValues[3] = ValueFactory.createValue(8.5);
	rowValues[4] = ValueFactory.createValue(true);
	rowGeom = ShapeFactory.createPoint2D(5, 5);
	updateRow = new DefaultRowEdited(new DefaultFeature(rowGeom, rowValues,
		rowValues[0].toString()), IRowEdited.STATUS_MODIFIED, 0);
    }

    public static DBLayerDefinition getLyrDef() {
	DBLayerDefinition testLyrDef = new DBLayerDefinition();
	testLyrDef.setName(TestUtils.testTableName);
	testLyrDef.setSchema("");
	testLyrDef.setTableName(TestUtils.testTableName);
	testLyrDef.setWhereClause("");
	testLyrDef.setShapeType(FShape.POINT);
	testLyrDef.setFieldGeometry(TestUtils.testTableGeomField);
	testLyrDef.setFieldID(TestUtils.testTableIdField);
	testLyrDef.setSRID_EPSG(TestUtils.testTableEpsg);
	// testLyrDef.setHost(TestUtils.testDataPath);
	// testLyrDef.setPort(0);
	// testLyrDef.setDataBase("");
	// testLyrDef.setUser("");
	// testLyrDef.setPassword("");
	// testLyrDef.setFieldNames(testTableAlphFields);
	testLyrDef.setFieldsDesc(getFieldDescription());
	return testLyrDef;
    }

    public static FieldDescription[] getFieldDescription() {
	FieldDescription[] fields = new FieldDescription[testTableAlphFields.length];

	fields[0] = new FieldDescription();
	fields[0].setFieldType(Types.INTEGER);
	fields[0].setFieldName("gid");
	fields[0].setFieldLength(20);

	fields[1] = new FieldDescription();
	fields[1].setFieldType(Types.VARCHAR);
	fields[1].setFieldName("texto");
	fields[1].setFieldLength(20);

	fields[2] = new FieldDescription();
	fields[2].setFieldType(Types.INTEGER);
	fields[2].setFieldName("entero");
	fields[2].setFieldLength(20);

	fields[3] = new FieldDescription();
	fields[3].setFieldType(Types.DOUBLE);
	fields[3].setFieldName("decimal");
	fields[3].setFieldLength(20);
	fields[3].setFieldDecimalCount(20);

	fields[4] = new FieldDescription();
	fields[4].setFieldType(Types.BOOLEAN);
	fields[4].setFieldName("bool");
	fields[4].setFieldLength(20);

	return fields;

    }

    public static void populateTable(Connection con, int nrows)
	    throws SQLException {
	String sql = "insert into test (entero, geom) values (?, MakePoint(?, ?, 32616))";
	PreparedStatement prep = con.prepareStatement(sql);

	int value = 0;
	for (int i = 0; i < nrows; i++) {
	    prep.setInt(1, value++);
	    prep.setDouble(2, Math.random() * 1000);
	    prep.setDouble(3, Math.random() * 1000);
	    prep.addBatch();
	}

	prep.executeBatch();
	con.commit();
	prep.close();
    }
}
