import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.IOException;

public class CreateGraphDatabaseFixture {

    private static final String URL = "remote:localhost";
    private static final String SERVER_USERNAME = "root";
    private static final String SERVER_PASSWORD = "root";
    static final String DB_USERNAME = "admin";
    static final String DB_PASSWORD = "admin";
    private static final String STORAGE_TYPE = "plocal";
    static final String DB_NAME = "testGraphDB";
    private static final String DB_TYPE = "graph";
    private OServerAdmin serverAdmin;
    OrientDB orientDB;
    ODatabaseSession graph;

    public static final Logger LOG = LoggerFactory.getLogger(CreateGraphDatabaseFixture.class);

    @BeforeTest
    public void createDatabase() throws IOException {
        serverAdmin = new OServerAdmin(URL);
        serverAdmin.connect(SERVER_USERNAME, SERVER_PASSWORD);
        orientDB = new OrientDB(URL, OrientDBConfig.defaultConfig());
        if (serverAdmin.existsDatabase(DB_NAME, STORAGE_TYPE)) {
            serverAdmin.dropDatabase(DB_NAME, STORAGE_TYPE);
        }
        serverAdmin.createDatabase(DB_NAME, DB_TYPE, STORAGE_TYPE);
    }

    @AfterTest
    public void dropDatabase() {
        try {
            serverAdmin.dropDatabase(DB_NAME, STORAGE_TYPE);
        } catch (IOException e) {
            LOG.error("DB was not dropped", e);
        }
    }
}
