import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Assert;
import org.testng.annotations.Test;
import utils.BasicUtils;
import utils.Counter;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static utils.BasicUtils.getTimeToInterrupt;

public class TransactionsTest extends CreateGraphDatabaseFixture {

    private static final String VERTEX_CLASS = "TestVertexClass";
    private static final String VERTEX_ID = "vertexId";
    private static final String CREATOR_ID = "creatorId";
    private static final String BATCH_COUNT = "batchCount";
    private static final String ITERATION = "iteration";
    private static final String RND = "rnd";
    private static final String EDGE_LABEL = "connects";

    @Test
    public void mainTest() throws InterruptedException, ExecutionException {
        graph = orientDB.open(DB_NAME, DB_USERNAME, DB_PASSWORD);

        OClass clazz = graph.createVertexClass(VERTEX_CLASS);
        graph.createEdgeClass(EDGE_LABEL);
        createProperties(clazz);
        createIndexes(clazz);
        graph.close();

        ExecutorService executor = Executors.newFixedThreadPool(8);
        List<Callable<Object>> tasksToCreate = new ArrayList<>();
        List<Callable<Object>> tasksToDelete = new ArrayList<>();
        AtomicBoolean interrupt = new AtomicBoolean(false);

        new Timer().schedule(
                new TimerTask() {
                    public void run() {
                        interrupt.set(true);
                    }
                },
                getTimeToInterrupt());

        try {
            for (int i = 0; i < 4; i++) {
                tasksToCreate.add(() -> {
                    long iterationNumber = 0;
                    try {
                        ODatabaseSession graph = orientDB.open(DB_NAME, DB_USERNAME, DB_PASSWORD);
                        while (!interrupt.get()) {
                            iterationNumber++;
                            addVertexesAndEdges(graph, iterationNumber);
                        }
                        graph.close();

                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }
            for (int i = 0; i < 4; i++) {
                tasksToDelete.add(() -> {
                    long iterationNumber = 0;
                    try {
                        ODatabaseSession graph = orientDB.open(DB_NAME, DB_USERNAME, DB_PASSWORD);
                        while (!interrupt.get()) {
                            iterationNumber++;
                            deleteVertexesAndEdges(graph, iterationNumber);
                        }
                        graph.close();

                        return null;
                    } catch (Exception e) {
                        LOG.error("Exception during operation processing", e);
                        throw e;
                    }
                });
            }

            tasksToCreate.addAll(tasksToDelete);

            List<Future<Object>> futures1 = executor.invokeAll(tasksToCreate);
            for (Future future : futures1) {
                future.get();
            }
        } finally {
            executor.shutdown();
        }
    }

    private void createProperties(OClass clazz) {
        clazz.createProperty(VERTEX_ID, OType.LONG);
        clazz.createProperty(CREATOR_ID, OType.INTEGER);
        clazz.createProperty(BATCH_COUNT, OType.INTEGER);
        clazz.createProperty(ITERATION, OType.LONG);
        clazz.createProperty(RND, OType.LONG);
    }

    private void createIndexes(OClass clazz) {
        clazz.createIndex(VERTEX_CLASS + "." + VERTEX_ID, OClass.INDEX_TYPE.NOTUNIQUE, VERTEX_ID);
        clazz.createIndex(VERTEX_CLASS + "." + RND, OClass.INDEX_TYPE.UNIQUE, RND);
        clazz.createIndex(VERTEX_CLASS + "." + ITERATION + "_" + CREATOR_ID + "_" + VERTEX_ID,
                OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, ITERATION, CREATOR_ID, VERTEX_ID);
    }

    private void addVertexesAndEdges(ODatabaseSession graph, long iterationNumber) {
        int batchSize = BasicUtils.generateBatchSize();
        List<OVertex> vertexes = new ArrayList<>(batchSize);
        List<Long> ids = new ArrayList<>();
        long threadId = Thread.currentThread().getId();
        graph.begin();
        try {
            for (int i = 0; i < batchSize; i++) {
                OVertex vertex = graph.newVertex(VERTEX_CLASS);
                long vertexId = Counter.getNextVertexId();
                ids.add(vertexId);
                vertex.setProperty(VERTEX_ID, vertexId);
                vertex.setProperty(CREATOR_ID, threadId);
                vertex.setProperty(BATCH_COUNT, batchSize);
                vertex.setProperty(ITERATION, iterationNumber);
                vertex.setProperty(RND, BasicUtils.generateRnd());
                vertex.save();
                vertexes.add(vertex);
                int addedVertexes = vertexes.size();

                OEdge edge;
                if (addedVertexes > 1) {
                    edge = graph.newEdge(vertexes.get(i - 1), vertexes.get(i), EDGE_LABEL);
                    edge.save();
                }
                if (addedVertexes == batchSize) {
                    edge = graph.newEdge(vertexes.get(i), vertexes.get(0), EDGE_LABEL);
                    edge.save();
                    checkRingCreated(graph, ids);
                }
                if (addedVertexes == batchSize / 3 || addedVertexes == batchSize * 2 / 3 || addedVertexes == batchSize) {
                    performSelectOperations(graph, ids, iterationNumber, threadId, ids.size(), 1);
                }
            }
            graph.commit();

            //actions after commit
            performSelectOperations(graph, ids, iterationNumber, threadId, ids.size(), 1);
            checkRingCreated(graph, ids);
            //checkClusterPositionsPositive(vertexes);
        } catch (ORecordDuplicatedException e) {
            LOG.error("Duplicated record", e);
            graph.rollback();

            //actions after rollback
            performSelectOperations(graph, ids, iterationNumber, threadId, 0, 0);
        } catch (Exception e) {
            LOG.error("Exception was caught");
            throw e;
        }
    }

    private void performSelectOperations(ODatabaseSession graph,
                                         List<Long> ids,
                                         long iteration,
                                         long threadId,
                                         int expectedAll,
                                         int expectedUnique) {

        long firstId = ids.get(0);
        long lastId = ids.get(ids.size() - 1);
        long limit = lastId - firstId + 1;

        OResultSet allRecords = graph.query(
                "select * from V where " + VERTEX_ID + " <= ? and " + ITERATION + " = ? and "
                        + CREATOR_ID + " = ? order by " + VERTEX_ID + " limit " + limit,
                lastId, iteration, threadId);

        Assert.assertEquals("Selecting of all vertexes returned a wrong number of records, # of ids " + ids.size(),
                expectedAll, allRecords.stream().count());

        for (long id : ids) {
            OResultSet uniqueItem = graph.query("select from V where " + VERTEX_ID + " = ?", id);
            Assert.assertEquals("Selecting by vertexId returned a wrong number of records",
                    expectedUnique, uniqueItem.stream().count());
        }
    }

    private void checkRingCreated(ODatabaseSession graph, List<Long> ids) {
        List<Integer> creatorIds = new ArrayList<>();
        List<Long> iterationNumbers = new ArrayList<>();

        long firstVertexId = ids.get(0);

        OResultSet resultSet = graph.query("select from V where " + VERTEX_ID + " = ?", firstVertexId);
        OVertex vertex = (OVertex) resultSet.next().getElement().get();

        int batchCount = vertex.getProperty(BATCH_COUNT);

        for (int i = 0; i < batchCount; i++) {
            creatorIds.add(vertex.getProperty(CREATOR_ID));
            iterationNumbers.add(vertex.getProperty(ITERATION));
            Iterable<OEdge> edges = vertex.getEdges(ODirection.OUT, EDGE_LABEL);
            Assert.assertTrue("Edge OUT doesn't exist in vertex " + vertex.getProperty(VERTEX_ID),
                    edges.iterator().hasNext());
            OVertex nextVertex = edges.iterator().next().getTo();

            long vertexId;
            if (i == batchCount - 1) {
                vertexId = ids.get(0);
            } else {
                vertexId = ids.get(i + 1);
            }

            boolean connected = nextVertex.getProperty(VERTEX_ID).equals(vertexId);
            Assert.assertTrue("Vertexes are not correctly connected by edges", connected);
            vertex = nextVertex;

        }
        boolean isOneThread = creatorIds.stream().distinct().limit(2).count() <= 1;
        boolean isOneIteration = iterationNumbers.stream().distinct().limit(2).count() <= 1;
        Assert.assertTrue("Vertexes are not created by one thread", isOneThread);
        Assert.assertTrue("Vertexes are not created during one iteration", isOneIteration);
    }

    private void checkClusterPositionsPositive(List<OVertex> vertexes) {
        for (int i = 0; i < vertexes.size(); i++) {
            long clusterPosition = vertexes.get(i).getIdentity().getClusterPosition();
            Assert.assertTrue("Cluster position in a record is not positive",
                    clusterPosition >= 0);
        }
    }

    private void deleteVertexesAndEdges(ODatabaseSession graph, long iterationNumber) {
        List<Long> ids = new ArrayList<>();
        long vertexIdToDelete = BasicUtils.getRandomVertexId();
        ids.add(vertexIdToDelete);

        OResultSet resultSet = graph.query("select from V where " + VERTEX_ID + " = ?", vertexIdToDelete);
        OVertex vertex = (OVertex) resultSet.next().getElement().get();
        int batchCount = vertex.getProperty(BATCH_COUNT);

        for (int i = 0; i < batchCount; i++) {
            Iterable<OEdge> edges = vertex.getEdges(ODirection.OUT, EDGE_LABEL);
            Assert.assertTrue("Edge OUT doesn't exist in vertex " + vertex.getProperty(VERTEX_ID),
                    edges.iterator().hasNext());
            OVertex nextVertex = edges.iterator().next().getTo();
            ids.add(nextVertex.getProperty(VERTEX_ID));
            vertex = nextVertex;
        }
        //TODO: not finished
    }
}