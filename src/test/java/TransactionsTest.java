import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.apache.commons.collections.CollectionUtils;
import org.testng.Assert;
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
        ODatabaseSession graphNoTx = orientDB.open(DB_NAME, DB_USERNAME, DB_PASSWORD);

        OClass clazz = graphNoTx.createVertexClass(VERTEX_CLASS);
        graphNoTx.createEdgeClass(EDGE_LABEL);
        createProperties(clazz);
        createIndexes(clazz);
        graphNoTx.close();

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
        int batchCount = BasicUtils.generateBatchSize();
        List<OVertex> vertexes = new ArrayList<>(batchCount);
        List<Long> ids = new ArrayList<>();

        graph.begin();
        long threadId = Thread.currentThread().getId();
        try {
            for (int i = 0; i < batchCount; i++) {
                OVertex vertex = graph.newVertex(VERTEX_CLASS);
                long vertexId = Counter.getNextVertexId();
                ids.add(vertexId);
                vertex.setProperty(VERTEX_ID, vertexId);
                vertex.setProperty(CREATOR_ID, threadId);
                vertex.setProperty(BATCH_COUNT, batchCount);
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
                if (addedVertexes == batchCount) {
                    edge = graph.newEdge(vertexes.get(i), vertexes.get(0), EDGE_LABEL);
                    edge.save();
                    checkRingCreated(graph, ids);
                }
                if (addedVertexes == batchCount / 3 || addedVertexes == batchCount * 2 / 3 || addedVertexes == batchCount) {
                    //performSelectOperations(graph, ids, iterationNumber, threadId, ids.size(), 1);
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

        selectAll(graph, ids, iteration, threadId, expectedAll);
        selectByIds(graph, ids, expectedUnique);
    }

    private void selectAll(ODatabaseSession graph, List<Long> ids, long iteration, long threadId, int expectedAll) {
        long firstId = ids.get(0);
        long lastId = ids.get(ids.size() - 1);
        long limit = lastId - firstId + 1;

        OResultSet allRecords = graph.query(
                "select * from V where " + VERTEX_ID + " <= ? and " + ITERATION + " = ? and "
                        + CREATOR_ID + " = ? order by " + VERTEX_ID + " limit " + limit,
                lastId, iteration, threadId);

        Assert.assertEquals(allRecords.stream().count(), expectedAll,
                "Selecting of all vertexes returned a wrong number of records, # of ids " + ids.size());
    }

    private void selectByIds(ODatabaseSession graph, List<Long> ids, int expectedUnique) {
        for (long id : ids) {
            OResultSet uniqueItem = graph.query("select from V where " + VERTEX_ID + " = ?", id);
            Assert.assertEquals(uniqueItem.stream().count(), expectedUnique,
                    "Selecting by vertexId returned a wrong number of records");
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
            Assert.assertTrue(edges.iterator().hasNext(),
                    "Edge OUT doesn't exist in vertex " + vertex.getProperty(VERTEX_ID));
            OVertex nextVertex = edges.iterator().next().getTo();

            long vertexId;
            if (i == batchCount - 1) {
                vertexId = ids.get(0);
            } else {
                vertexId = ids.get(i + 1);
            }

            boolean connected = nextVertex.getProperty(VERTEX_ID).equals(vertexId);
            Assert.assertTrue(connected, "Vertexes are not correctly connected by edges");
            vertex = nextVertex;

        }
        boolean isOneThread = creatorIds.stream().distinct().limit(2).count() <= 1;
        boolean isOneIteration = iterationNumbers.stream().distinct().limit(2).count() <= 1;
        Assert.assertTrue(isOneThread, "Vertexes are not created by one thread");
        Assert.assertTrue(isOneIteration, "Vertexes are not created during one iteration");
    }

    private void checkClusterPositionsPositive(List<OVertex> vertexes) {
        for (int i = 0; i < vertexes.size(); i++) {
            long clusterPosition = vertexes.get(i).getIdentity().getClusterPosition();
            Assert.assertTrue(clusterPosition >= 0, "Cluster position in a record is not positive");
        }
    }

    private void deleteVertexesAndEdges(ODatabaseSession graph, long iterationNumber) {
        graph.begin();

        long firstVertex = BasicUtils.getRandomVertexId();
        OResultSet resultSet = graph.query("select from V where " + VERTEX_ID + " = ?", firstVertex);
        OVertex vertex = (OVertex) resultSet.next().getElement().get();
        int batchCount = vertex.getProperty(BATCH_COUNT);
        int threadId = vertex.getProperty(CREATOR_ID);

        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < batchCount; i++) {
            Iterable<OEdge> edges = vertex.getEdges(ODirection.OUT, EDGE_LABEL);
            Assert.assertTrue(edges.iterator().hasNext(),
                    "Edge OUT doesn't exist in vertex " + vertex.getProperty(VERTEX_ID));
            OVertex nextVertex = edges.iterator().next().getTo();
            ids.add(nextVertex.getProperty(VERTEX_ID));
            vertex = nextVertex;
        }
        Collections.sort(ids);

        List<Long> deletedIds = new ArrayList<>();
        for (int i = 0; i < batchCount; i++) {
            long idToDelete = ids.get(i);
            OResultSet result = graph.command("delete vertex V where " + VERTEX_ID + " = ?", idToDelete);
            long deletedCount = result.next().getProperty("count");
            Assert.assertEquals(deletedCount, 1, "Vertex was not deleted; ");
            deletedIds.add(idToDelete);
            int deletedIdsSize = deletedIds.size();
            if (deletedIdsSize == batchCount / 3 || deletedIdsSize == batchCount * 2 / 3 || deletedIdsSize == batchCount) {
                //check whether a part of vertexes were really deleted
                selectByIds(graph, deletedIds, 0);
                //check whether all the rest of the vertexes persist
                List<Long> retainedIds = (List<Long>) CollectionUtils.disjunction(ids, deletedIds);
                if (!retainedIds.isEmpty()) {
                    performSelectOperations(
                            graph, retainedIds, iterationNumber, threadId, retainedIds.size(), 1);
                }
            }
        }
        graph.commit();
    }
}