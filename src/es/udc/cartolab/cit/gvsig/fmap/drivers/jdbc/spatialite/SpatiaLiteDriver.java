package es.udc.cartolab.cit.gvsig.fmap.drivers.jdbc.spatialite;

import java.awt.geom.Rectangle2D;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.sqlite.SQLiteConnection;
import org.sqlite.core.CoreConnection;

import com.hardcode.gdbms.driver.exceptions.InitializeWriterException;
import com.hardcode.gdbms.driver.exceptions.ReadDriverException;
import com.hardcode.gdbms.driver.exceptions.ReloadDriverException;
import com.hardcode.gdbms.engine.data.edition.DataWare;
import com.hardcode.gdbms.engine.values.Value;
import com.hardcode.gdbms.engine.values.ValueFactory;
import com.iver.cit.gvsig.fmap.core.FShape;
import com.iver.cit.gvsig.fmap.core.ICanReproject;
import com.iver.cit.gvsig.fmap.core.IGeometry;
import com.iver.cit.gvsig.fmap.drivers.ConnectionJDBC;
import com.iver.cit.gvsig.fmap.drivers.DBException;
import com.iver.cit.gvsig.fmap.drivers.DBLayerDefinition;
import com.iver.cit.gvsig.fmap.drivers.DefaultJDBCDriver;
import com.iver.cit.gvsig.fmap.drivers.DriverAttributes;
import com.iver.cit.gvsig.fmap.drivers.FieldDescription;
import com.iver.cit.gvsig.fmap.drivers.IConnection;
import com.iver.cit.gvsig.fmap.drivers.IFeatureIterator;
import com.iver.cit.gvsig.fmap.drivers.WKBParser3;
import com.iver.cit.gvsig.fmap.edition.IWriteable;
import com.iver.cit.gvsig.fmap.edition.IWriter;

public class SpatiaLiteDriver extends DefaultJDBCDriver implements
ICanReproject, IWriteable {

    private static Logger logger = Logger.getLogger(SpatiaLiteDriver.class
	    .getName());
    public static final String NAME = "SpatiaLite JDBC Driver";

    protected static Map<String, Connection> conns = new HashMap<String, Connection>();
    protected static String latestHost = null;
    private static boolean dependenciesChecked = false;
    private WKBParser3 parser = new WKBParser3();
    private String originalEPSG = null;
    private static int FETCH_SIZE = 5000;
    private int fetch_min = -1;
    private int fetch_max = -1;
    private String sqlTotal;
    private String completeWhere;
    private String sqlOrig;
    private String strEPSG = null;
    private SpatiaLite spatiaLite = new SpatiaLite();
    private SpatiaLiteWriter writer = new SpatiaLiteWriter();

    public SpatiaLiteDriver() {
	super();
	loadSystemDependencies();
    }

    public static Connection getConnection(String host) {
	if (!conns.containsKey(host)) {
	    return null;
	}
	return conns.get(host);
    }

    public static void updateConnection(String host, Connection conn) {
	conns.put(host, conn);
    }

    public static void closeAllConnections() {
	for (Connection conn : conns.values()) {
	    if (conn instanceof SQLiteConnection) {
		try {
		    ((CoreConnection) conn).realClose();
		} catch (SQLException e) {
		    e.printStackTrace();
		}
	    }
	}
	conns.clear();
    }

    private void loadSystemDependencies() {
	if (!dependenciesChecked) {
	    try {
		Class.forName("org.sqlite.JDBC");
	    } catch (ClassNotFoundException e) {
		throw new RuntimeException(e);
	    }
	    // We try to manually load the SQLite library and all the
	    // dependencies needed by it and SpatiaLite
	    new NativeDependencies().loadSystemDependencies();
	    dependenciesChecked = true;
	}
    }

    @Override
    public void reload() throws ReloadDriverException {
	try {
	    if ((conn == null)
		    || (((ConnectionJDBC) conn).getConnection().isClosed())) {
		this.load();
	    }

	    setData(conn, lyrDef);
	} catch (SQLException e) {
	    throw new ReloadDriverException(getName(), e);
	} catch (ReadDriverException e) {
	    throw new ReloadDriverException(getName(), e);
	} catch (DBException e) {
	    throw new ReloadDriverException(getName(), e);
	}

    }

    @Override
    public void open() {
    }

    public boolean closeResultSet(ResultSet rs) {
	boolean error = false;

	if (rs != null) {
	    try {
		rs.close();
		error = true;
	    } catch (SQLException e) {
		logger.error(e.getMessage(), e);
	    }
	}

	return error;
    }

    public boolean closeStatement(Statement st) {
	boolean error = false;

	if (st != null) {
	    try {
		st.close();
		error = true;
	    } catch (SQLException e) {
		logger.error(e.getMessage(), e);
	    }
	}

	return error;
    }

    public boolean closeConnection(IConnection conn) {
	boolean error = false;

	if (conn != null) {
	    try {
		conn.close();
		error = true;
	    } catch (DBException e) {
		logger.error(e.getMessage(), e);
	    }
	}

	return error;
    }

    @Override
    public String[] getGeometryFieldsCandidates(IConnection conn,
	    String table_name) throws DBException {
	List<String> list = new ArrayList<String>();
	try {
	    String sql = "SELECT * FROM GEOMETRY_COLUMNS WHERE F_TABLE_NAME = ?";
	    PreparedStatement stAux = getConnection(conn).prepareStatement(sql);
	    stAux.setString(1, table_name);

	    ResultSet rs = stAux.executeQuery();
	    while (rs.next()) {
		String geomCol = rs.getString("F_GEOMETRY_COLUMN");
		list.add(geomCol);
	    }
	    rs.close();
	    stAux.close();
	} catch (SQLException e) {
	    closeConnection(conn);
	    throw new DBException(e);
	}
	return list.toArray(new String[0]);
    }

    @Override
    public String getGeometryField(String fieldName) {
	return "asBinary(\"" + fieldName + "\")";
    }

    @Override
    public int getDefaultPort() {
	return 0;
    }

    @Override
    public String getConnectionStringBeginning() {
	return spatiaLite.getConnectionStringBeginning();
    }

    @Override
    public String getConnectionString(String host, String port, String dbname,
	    String user, String pw) {
	return getConnectionStringBeginning() + host;
    }

    @Override
    public void setData(IConnection conn, DBLayerDefinition lyrDef)
	    throws DBException {

	try {
	    setConnection(conn);

	    lyrDef.setConnection(conn);
	    setLyrDef(lyrDef);

	    getTableEPSG_and_shapeType(conn, lyrDef);

	    getLyrDef().setSRID_EPSG(originalEPSG);

	    sqlOrig = "SELECT " + getTotalFields() + " FROM "
		    + getLyrDef().getComposedTableName() + " ";
	    if (canReproject(strEPSG)) {
		completeWhere = getCompoundWhere(sqlOrig, workingArea, strEPSG);
	    } else {
		completeWhere = getCompoundWhere(sqlOrig, workingArea,
			originalEPSG);
	    }

	    String sqlAux = sqlOrig + completeWhere + " ORDER BY "
		    + getLyrDef().getFieldID();

	    sqlTotal = sqlAux;

	    Statement st = getConnection(conn).createStatement();
	    rs = st.executeQuery(sqlAux + " LIMIT " + FETCH_SIZE);
	    fetch_min = 0;
	    fetch_max = FETCH_SIZE - 1;
	    metaData = rs.getMetaData();
	    doRelateID_FID();

	    writer.setCreateTable(false);
	    writer.setWriteAll(false);
	    writer.initialize(lyrDef);

	} catch (SQLException e) {

	    try {
		getConnection(conn).rollback();
	    } catch (SQLException e1) {
		logger.warn("Unable to rollback connection after problem ("
			+ e.getMessage() + ") in setData()");
	    }

	    try {
		if (rs != null) {
		    rs.close();
		}
	    } catch (SQLException e1) {
		throw new DBException(e);
	    }
	    throw new DBException(e);
	} catch (InitializeWriterException e) {
	    throw new DBException(e);
	}

    }

    protected void setConnection(IConnection conn) throws DBException,
    SQLException {
	final Connection javaCon = getConnection(conn);
	if (!(javaCon instanceof SQLiteConnection)) {
	    throw new RuntimeException("Not a SQLiteConnection");
	}
	this.conn = conn;
	boolean loadSpatialite = !conns.containsValue(javaCon);

	conns.put(getHost(conn), javaCon);
	javaCon.setAutoCommit(false);

	if (loadSpatialite) {
	    new NativeDependencies().loadSpatialite(javaCon);
	}

    }

    private String getHost(IConnection conn) throws DBException {
	return conn.getURL().replaceFirst(getConnectionStringBeginning(), "");
    }

    @Override
    public String[] getAllFields(IConnection conn, String table_name)
	    throws DBException {
	Statement st = null;
	ResultSet rs = null;

	try {
	    st = getConnection(conn).createStatement();
	    rs = st.executeQuery("SELECT * FROM \""
		    + table_name.replace("\"", "\\\"") + "\" LIMIT 1");
	    ResultSetMetaData rsmd = rs.getMetaData();
	    String[] ret = new String[rsmd.getColumnCount()];

	    for (int i = 0; i < ret.length; i++) {
		ret[i] = rsmd.getColumnName(i + 1);
	    }

	    return ret;
	} catch (SQLException e) {
	    closeConnection(conn);
	    throw new DBException(e);
	} finally {
	    closeResultSet(rs);
	    closeStatement(st);
	}
    }

    @Override
    public String[] getAllFieldTypeNames(IConnection conn, String table_name)
	    throws DBException {
	Statement st = null;
	ResultSet rs = null;
	try {
	    st = getConnection(conn).createStatement();
	    rs = st.executeQuery("SELECT * FROM \""
		    + table_name.replace("\"", "\\\"") + "\" LIMIT 1");
	    ResultSetMetaData rsmd = rs.getMetaData();
	    String[] ret = new String[rsmd.getColumnCount()];

	    for (int i = 0; i < ret.length; i++) {
		ret[i] = rsmd.getColumnTypeName(i + 1);
	    }
	    return ret;
	} catch (SQLException e) {
	    closeConnection(conn);
	    throw new DBException(e);
	} finally {
	    closeStatement(st);
	    closeResultSet(rs);
	}
    }

    @Override
    public DriverAttributes getDriverAttributes() {
	return null;
    }

    @Override
    public boolean isWritable() {
	return writer.canSaveEdits();
    }

    @Override
    public String getSqlTotal() {
	return sqlTotal;
    }

    @Override
    public String getCompleteWhere() {
	return completeWhere;
    }

    private String getCompoundWhere(String sql, Rectangle2D r, String strEPSG) {
	if (r == null) {
	    return getWhereClause();
	}

	double xMin = r.getMinX();
	double yMin = r.getMinY();
	double xMax = r.getMaxX();
	double yMax = r.getMaxY();
	String wktBox = "geomfromewkt('POLYGON((" + xMin + " " + yMin + ", "
		+ xMax + " " + yMin + ", " + xMax + " " + yMax + ", " + xMin
		+ " " + yMax + ", " + xMin + " " + yMin + "))')";
	String sqlAux;
	if (getWhereClause().toUpperCase().indexOf("WHERE") != -1) {
	    sqlAux = getWhereClause() + " AND (Intersects(\""
		    + getLyrDef().getFieldGeometry() + "\", " + wktBox
		    + ") = 1)";
	} else {
	    sqlAux = "WHERE (Intersects(\"" + getLyrDef().getFieldGeometry()
		    + "\", " + wktBox + ") = 1)";
	}
	return sqlAux;
    }

    @Override
    public IFeatureIterator getFeatureIterator(String sql)
	    throws ReadDriverException {
	SpatiaLiteFeatureIterator geomIterator = null;
	geomIterator = myGetFeatureIterator(sql);
	geomIterator.setLyrDef(getLyrDef());

	return geomIterator;
    }

    @Override
    public IFeatureIterator getFeatureIterator(Rectangle2D r, String strEPSG)
	    throws ReadDriverException {
	if (workingArea != null) {
	    r = r.createIntersection(workingArea);
	}

	String sqlAux;
	if (canReproject(strEPSG)) {
	    sqlAux = sqlOrig + getCompoundWhere(sqlOrig, r, strEPSG);
	} else {
	    sqlAux = sqlOrig + getCompoundWhere(sqlOrig, r, originalEPSG);
	}

	return getFeatureIterator(sqlAux);
    }

    /**
     * Recorre el recordset creando una tabla Hash que usaremos para relacionar
     * el número de un registro con su identificador único. Debe ser llamado en
     * el setData justo después de crear el recorset principal
     *
     * @throws SQLException
     */
    @Override
    protected void doRelateID_FID() throws DBException {
	try {
	    hashRelate = new Hashtable();

	    String strSQL = "SELECT " + getLyrDef().getFieldID() + " FROM "
		    + getLyrDef().getComposedTableName() + " "
		    + getCompleteWhere() + " ORDER BY "
		    + getLyrDef().getFieldID();
	    Statement s = ((ConnectionJDBC) getConnection()).getConnection()
		    .createStatement();
	    ResultSet r = s.executeQuery(strSQL);
	    int index = 0;
	    while (r.next()) {
		String aux = r.getString(1);
		Value val = ValueFactory.createValue(aux);
		hashRelate.put(val, new Integer(index));
		index++;
	    }
	    numReg = index;
	    r.close();
	    // rs.beforeFirst();
	} catch (SQLException e) {
	    throw new DBException(e);
	}

    }

    private DBLayerDefinition cloneLyrDef(DBLayerDefinition lyrDef) {
	DBLayerDefinition clonedLyrDef = new DBLayerDefinition();

	clonedLyrDef.setName(lyrDef.getName());
	clonedLyrDef.setFieldsDesc(lyrDef.getFieldsDesc());

	clonedLyrDef.setShapeType(lyrDef.getShapeType());
	clonedLyrDef.setProjection(lyrDef.getProjection());

	clonedLyrDef.setConnection(lyrDef.getConnection());
	clonedLyrDef.setCatalogName(lyrDef.getCatalogName());
	clonedLyrDef.setSchema(lyrDef.getSchema());
	clonedLyrDef.setTableName(lyrDef.getTableName());

	clonedLyrDef.setFieldID(lyrDef.getFieldID());
	clonedLyrDef.setFieldGeometry(lyrDef.getFieldGeometry());
	clonedLyrDef.setWhereClause(lyrDef.getWhereClause());
	clonedLyrDef.setWorkingArea(lyrDef.getWorkingArea());
	clonedLyrDef.setSRID_EPSG(lyrDef.getSRID_EPSG());
	clonedLyrDef.setClassToInstantiate(lyrDef.getClassToInstantiate());

	clonedLyrDef.setIdFieldID(lyrDef.getIdFieldID());
	clonedLyrDef.setDimension(lyrDef.getDimension());
	clonedLyrDef.setHost(lyrDef.getHost());
	clonedLyrDef.setPort(lyrDef.getPort());
	clonedLyrDef.setDataBase(lyrDef.getDataBase());
	clonedLyrDef.setUser(lyrDef.getUser());
	clonedLyrDef.setPassword(lyrDef.getPassword());
	clonedLyrDef.setConnectionName(lyrDef.getConnectionName());
	return clonedLyrDef;
    }

    @Override
    public IFeatureIterator getFeatureIterator(Rectangle2D r, String strEPSG,
	    String[] alphaNumericFieldsNeeded) throws ReadDriverException {
	String sqlAux = null;
	DBLayerDefinition lyrDef = getLyrDef();
	DBLayerDefinition clonedLyrDef = cloneLyrDef(lyrDef);
	ArrayList<FieldDescription> myFieldsDesc = new ArrayList<FieldDescription>(); // =
	if (workingArea != null) {
	    r = r.createIntersection(workingArea);
	}
	String strAux = getGeometryField(lyrDef.getFieldGeometry());

	boolean found = false;
	int fieldIndex = -1;
	if (alphaNumericFieldsNeeded != null) {
	    FieldDescription[] fieldsDesc = lyrDef.getFieldsDesc();

	    for (int i = 0; i < alphaNumericFieldsNeeded.length; i++) {
		fieldIndex = lyrDef
			.getFieldIdByName(alphaNumericFieldsNeeded[i]);
		if (fieldIndex < 0) {
		    throw new RuntimeException(
			    "No se ha encontrado el nombre de campo "
				    + alphaNumericFieldsNeeded[i]);
		}
		strAux = strAux
			+ ", "
			+ SpatiaLite
			.escapeFieldName(lyrDef.getFieldNames()[fieldIndex]);
		if (alphaNumericFieldsNeeded[i].equalsIgnoreCase(lyrDef
			.getFieldID())) {
		    found = true;
		    clonedLyrDef.setIdFieldID(i);
		}

		myFieldsDesc.add(fieldsDesc[fieldIndex]);
	    }
	}
	// Nos aseguramos de pedir siempre el campo ID
	if (found == false) {
	    strAux = strAux + ", " + lyrDef.getFieldID();
	    myFieldsDesc.add(lyrDef.getFieldsDesc()[lyrDef.getIdField(lyrDef
		    .getFieldID())]);
	    clonedLyrDef.setIdFieldID(myFieldsDesc.size() - 1);
	}
	clonedLyrDef.setFieldsDesc(myFieldsDesc
		.toArray(new FieldDescription[] {}));

	String sqlProv = "SELECT " + strAux + " FROM "
		+ lyrDef.getComposedTableName() + " ";

	if (canReproject(strEPSG)) {
	    sqlAux = sqlProv + getCompoundWhere(sqlProv, r, strEPSG);
	} else {
	    sqlAux = sqlProv + getCompoundWhere(sqlProv, r, originalEPSG);
	}

	System.out.println("SqlAux getFeatureIterator = " + sqlAux);
	SpatiaLiteFeatureIterator geomIterator = null;
	geomIterator = myGetFeatureIterator(sqlAux);
	geomIterator.setLyrDef(clonedLyrDef);
	return geomIterator;
    }

    private SpatiaLiteFeatureIterator myGetFeatureIterator(String sql)
	    throws ReadDriverException {
	SpatiaLiteFeatureIterator geomIterator = null;
	try {
	    geomIterator = new SpatiaLiteFeatureIterator(
		    ((ConnectionJDBC) conn).getConnection(), sql);
	} catch (SQLException e) {
	    throw new ReadDriverException("SpatiaLite Driver", e);
	}
	return geomIterator;
    }

    private void setAbsolutePosition(int index) throws SQLException {

	if (rs == null) {
	    try {
		reload();
	    } catch (Exception e) {
		e.printStackTrace();
		throw new SQLException(e);
	    }
	}

	if ((index < fetch_min) || (index > fetch_max)) {
	    // calculamos el intervalo correcto
	    fetch_min = index;
	    fetch_max = fetch_min + FETCH_SIZE - 1;
	    // y cogemos ese cacho
	    rs.close();

	    Statement st = ((ConnectionJDBC) conn).getConnection()
		    .createStatement(ResultSet.TYPE_FORWARD_ONLY,
			    ResultSet.CONCUR_READ_ONLY);

	    rs = st.executeQuery(sqlTotal + " LIMIT " + FETCH_SIZE + " OFFSET "
		    + fetch_min);
	}
	int diff = index - rs.getRow() + 1;
	if (diff < 0) {
	    Statement st = ((ConnectionJDBC) conn).getConnection()
		    .createStatement(ResultSet.TYPE_FORWARD_ONLY,
			    ResultSet.CONCUR_READ_ONLY);
	    rs = st.executeQuery(sqlTotal + " LIMIT " + FETCH_SIZE + " OFFSET "
		    + fetch_min);
	    diff = index - rs.getRow() + 1;
	    for (int i = diff; i > 0; i--) {
		rs.next();
	    }
	} else {
	    for (int i = diff; i > 0; i--) {
		rs.next();
	    }
	}

    }

    public String getPrimaryKey(IConnection con, String table_name) {

	String query = "SELECT rowid FROM \""
		+ table_name.replace("\"", "\\\"") + "\" LIMIT 1;";

	try {

	    Connection c = getConnection(con);
	    PreparedStatement st = c.prepareStatement(query);

	    ResultSet rs = st.executeQuery();

	    String primaryKey = "";
	    if (rs.next()) {
		primaryKey = rs.getMetaData().getColumnName(1);
	    }

	    rs.close();
	    st.close();

	    return primaryKey;
	} catch (SQLException e) {
	    try {
		con.close();
	    } catch (DBException e2) {
		e.printStackTrace();
	    }
	    return "";
	}
    }

    @Override
    public IGeometry getShape(int index) throws ReadDriverException {
	IGeometry geom = null;
	try {
	    setAbsolutePosition(index);
	    if (rs != null) {
		byte[] data = rs.getBytes(1);
		if (data == null) {
		    return null;
		}
		geom = parser.parse(data);
	    }
	} catch (SQLException e) {
	    throw new ReadDriverException(this.getName(), e);
	}

	return geom;
    }

    @Override
    public Value getFieldValue(long rowIndex, int idField)
	    throws ReadDriverException {
	int index = (int) (rowIndex);
	try {
	    setAbsolutePosition(index);
	    int fieldId = idField + 2;
	    Value auxValue = getFieldValue(rs, fieldId);
	    // There is a minor problem with the id value, because it's read
	    // as a double (e.g. 1.0) but it was stored inside the hashmap
	    // as an integer, so here we have to transform it into an int
	    if (getLyrDef().getIdFieldID() == idField) {
		try {
		    auxValue = ValueFactory.createValue(new Double(auxValue
			    .toString()).intValue());
		} catch (NumberFormatException e) {
		}
	    }
	    return auxValue;
	} catch (SQLException e) {
	    throw new ReadDriverException("SpatiaLite Driver", e);
	}
    }

    @Override
    public Rectangle2D getFullExtent() throws ReadDriverException {
	if (fullExtent == null) {
	    try {
		Statement s = ((ConnectionJDBC) conn).getConnection()
			.createStatement();
		String query = "SELECT asBinary(extent(\""
			+ getLyrDef().getFieldGeometry() + "\")) FROM "
			+ getLyrDef().getComposedTableName() + " "
			+ getCompleteWhere();
		ResultSet r = s.executeQuery(query);
		r.next();
		byte[] geomValue = r.getBytes(1);
		if (geomValue == null) {
		    logger.debug("La capa " + getLyrDef().getName()
			    + " no tiene FULLEXTENT");
		    return null;
		}
		fullExtent = parser.parse(geomValue).getBounds2D();
	    } catch (SQLException e) {
		throw new ReadDriverException(this.getName(), e);
	    }

	}

	return fullExtent;
    }

    private void getTableEPSG_and_shapeType(IConnection conn,
	    DBLayerDefinition dbld) {
	try {
	    Statement stAux = getConnection(conn).createStatement();

	    String sql = "SELECT * FROM GEOMETRY_COLUMNS WHERE F_TABLE_NAME = '"
		    + dbld.getTableName()
		    + "' AND F_GEOMETRY_COLUMN = '"
		    + dbld.getFieldGeometry() + "'";

	    ResultSet rs = stAux.executeQuery(sql);
	    if (rs.next()) {
		originalEPSG = "" + rs.getInt("SRID");
		String geometryType = rs.getString("geometry_type");
		int shapeType = FShape.MULTI;
		if (geometryType.compareToIgnoreCase("POINT") == 0) {
		    shapeType = FShape.POINT;
		} else if (geometryType.compareToIgnoreCase("LINESTRING") == 0) {
		    shapeType = FShape.LINE;
		} else if (geometryType.compareToIgnoreCase("POLYGON") == 0) {
		    shapeType = FShape.POLYGON;
		} else if (geometryType.compareToIgnoreCase("MULTIPOINT") == 0) {
		    shapeType = FShape.MULTIPOINT;
		} else if (geometryType.compareToIgnoreCase("MULTILINESTRING") == 0) {
		    shapeType = FShape.LINE;
		} else if (geometryType.compareToIgnoreCase("MULTILINESTRINGM") == 0) {
		    shapeType = FShape.LINE | FShape.M;
		} else if (geometryType.compareToIgnoreCase("MULTIPOLYGON") == 0) {
		    shapeType = FShape.POLYGON;
		}
		dbld.setShapeType(shapeType);

		int dimension = rs.getString("COORD_DIMENSION").length();
		dbld.setDimension(dimension);

	    } else {
		originalEPSG = "-1";
	    }

	    rs.close();
	} catch (SQLException e) {
	    originalEPSG = "-1";
	    logger.error(e);
	    e.printStackTrace();
	}

    }

    public static Value getFieldValue(ResultSet aRs, int fieldId)
	    throws SQLException {
	ResultSetMetaData metaData = aRs.getMetaData();
	byte[] byteBuf = aRs.getBytes(fieldId);
	Value value = null;
	if (byteBuf == null) {
	    return ValueFactory.createNullValue();
	} else {
	    int colType = metaData.getColumnType(fieldId);

	    switch (colType) {
	    case Types.BIGINT:
		value = ValueFactory.createValue(aRs.getLong(fieldId));

		break;

	    case Types.BIT:
	    case Types.BOOLEAN:
		value = ValueFactory.createValue(aRs.getBoolean(fieldId));

		break;

	    case Types.CHAR:
	    case Types.VARCHAR:
	    case Types.LONGVARCHAR:
		String auxString = aRs.getString(fieldId);
		if (auxString != null) {
		    value = ValueFactory.createValue(auxString);
		}

		break;

	    case Types.DATE:
		Date auxDate = aRs.getDate(fieldId);
		if (auxDate != null) {
		    value = ValueFactory.createValue(auxDate);
		}

		break;

	    case Types.DECIMAL:
	    case Types.NUMERIC:
	    case Types.FLOAT:
	    case Types.DOUBLE:
		value = ValueFactory.createValue(aRs.getDouble(fieldId));

		break;

	    case Types.INTEGER:
		value = ValueFactory.createValue(aRs.getInt(fieldId));

		break;

	    case Types.REAL:
		value = ValueFactory.createValue(aRs.getFloat(fieldId));

		break;

	    case Types.SMALLINT:
		value = ValueFactory.createValue(aRs.getShort(fieldId));

		break;

	    case Types.TINYINT:
		value = ValueFactory.createValue(aRs.getByte(fieldId));

		break;

	    case Types.BINARY:
	    case Types.VARBINARY:
	    case Types.LONGVARBINARY:
		byte[] auxByteArray = aRs.getBytes(fieldId);
		if (auxByteArray != null) {
		    value = ValueFactory.createValue(auxByteArray);
		}

		break;

	    case Types.TIMESTAMP:
		try {
		    Timestamp auxTimeStamp = aRs.getTimestamp(fieldId);
		    value = ValueFactory.createValue(auxTimeStamp);
		} catch (SQLException e) {
		    value = ValueFactory.createValue(new Timestamp(0));
		}

		break;

	    case Types.TIME:
		try {
		    Time auxTime = aRs.getTime(fieldId);
		    value = ValueFactory.createValue(auxTime);
		} catch (SQLException e) {
		    value = ValueFactory.createValue(new Time(0));
		}

		break;

	    default:
		Object _obj = null;
		try {
		    _obj = aRs.getObject(fieldId);
		} catch (Exception ex) {
		    logger.error("Error getting object: " + ex.getMessage());
		}
		if (_obj == null) {
		    value = ValueFactory.createValue("");
		} else {
		    value = ValueFactory.createValue(_obj.toString());
		}
	    }

	    if (aRs.wasNull()) {
		return ValueFactory.createNullValue();
	    } else {
		return value;
	    }
	}

    }

    @Override
    public String getName() {
	return NAME;
    }

    @Override
    public int[] getPrimaryKeys() {
	return null;
    }

    @Override
    public void write(DataWare dataWare) {
    }

    @Override
    public String getSourceProjection(IConnection conn, DBLayerDefinition dbld) {
	if (originalEPSG == null) {
	    getTableEPSG_and_shapeType(conn, dbld);
	}
	return originalEPSG;
    }

    @Override
    public String getDestProjection() {
	return strEPSG;
    }

    @Override
    public void setDestProjection(String toEPSG) {
	this.strEPSG = toEPSG;

    }

    @Override
    public boolean canRead(IConnection iconn, String tablename) {
	return true;
    }

    @Override
    public boolean canReproject(String toEPSGdestinyProjection) {
	return false;
    }

    @Override
    public IWriter getWriter() {
	return writer;
    }

    private Connection getConnection(IConnection conn) {
	return ((ConnectionJDBC) conn).getConnection();
    }

}
