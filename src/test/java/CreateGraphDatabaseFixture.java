import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
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

    @BeforeTest
    public void createDatabase() {
        try {
            serverAdmin = new OServerAdmin(URL);
            serverAdmin.connect(USERNAME, PASSWORD);
            if (serverAdmin.existsDatabase(DB_NAME, STORAGE_TYPE)) {
                serverAdmin.dropDatabase(DB_NAME, STORAGE_TYPE);
            }
            serverAdmin.createDatabase(DB_NAME, DB_TYPE, STORAGE_TYPE);
        } catch (IOException e) {
            e.printStackTrace();
        }

        factory = new OrientGraphFactory(URL + "/" + DB_NAME, USERNAME, PASSWORD);
    }

    @AfterTest
    public void dropDatabase() {
        try {
            serverAdmin.dropDatabase(DB_NAME, STORAGE_TYPE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
