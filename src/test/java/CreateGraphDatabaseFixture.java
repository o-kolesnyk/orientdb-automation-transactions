import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;

import java.io.IOException;

public class CreateGraphDatabaseFixture {

    private static final String URL = "remote:localhost";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";
    private static final String STORAGE_TYPE = "plocal";
    private static final String DB_NAME = "testGraphDB";
    private static final String DB_TYPE = "graph";
    private OServerAdmin serverAdmin;
    OrientGraphFactory factory;

    public static final Logger LOG = LoggerFactory.getLogger(CreateGraphDatabaseFixture.class);

    @BeforeTest
    public void createDatabase() throws IOException {
        serverAdmin = new OServerAdmin(URL);
        serverAdmin.connect(USERNAME, PASSWORD);
        if (serverAdmin.existsDatabase(DB_NAME, STORAGE_TYPE)) {
            serverAdmin.dropDatabase(DB_NAME, STORAGE_TYPE);
        }
        serverAdmin.createDatabase(DB_NAME, DB_TYPE, STORAGE_TYPE);
        factory = new OrientGraphFactory(URL + "/" + DB_NAME, USERNAME, PASSWORD);
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
