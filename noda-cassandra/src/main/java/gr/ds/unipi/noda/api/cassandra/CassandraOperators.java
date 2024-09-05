package gr.ds.unipi.noda.api.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import gr.ds.unipi.noda.api.cassandra.filterOperators.geoperators.geoTemporalOperators.GeoTemporalOperator;
import gr.ds.unipi.noda.api.cassandra.filterOperators.geoperators.geoTemporalOperators.OperatorInGeoTemporalRectangle;
import gr.ds.unipi.noda.api.cassandra.filterOperators.geoperators.geoTextualOperators.geoTextualConstraintOperators.GeoTextualConstraintOperator;
import gr.ds.unipi.noda.api.cassandra.filterOperators.geoperators.geoTextualOperators.geoTextualConstraintOperators.OperatorInGeoTextualRectangle;
import gr.ds.unipi.noda.api.cassandra.filterOperators.geoperators.geographicalOperators.GeographicalOperator;
import gr.ds.unipi.noda.api.cassandra.filterOperators.geoperators.geographicalOperators.OperatorInGeoRectangle;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbConnector;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbResults;
import gr.ds.unipi.noda.api.core.operators.Operator;
import gr.ds.unipi.noda.api.core.operators.aggregateOperators.AggregateOperator;
import gr.ds.unipi.noda.api.core.operators.filterOperators.FilterOperator;
import gr.ds.unipi.noda.api.core.operators.filterOperators.geoperators.geometries.Geometry;
import gr.ds.unipi.noda.api.core.operators.joinOperators.JoinOperator;
import gr.ds.unipi.noda.api.core.operators.sortOperators.SortOperator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

final class CassandraOperators extends NoSqlDbOperators {

    private final CassandraConnectionManager cassandraConnectionManager = CassandraConnectionManager.getInstance();
    //to access the database's object for connectivity, call cassandraConnectionManager.getConnection(getNoSqlDbConnector())
    //to access the defined name of collection or table, call  getDataCollection()
    //to access the SparkSession, call getSparkSession()
    private ArrayList<String>[] selectClauseList;
    private ArrayList<String> selectClauseListUSO;
    private ArrayList<String> selectClauseListGeo;
    private ArrayList<String> whereClauseList;
    private ArrayList<String> groupByClauseList;
    private ArrayList<String> orderByClauseList;
    private int limit;
    private CassandraQueryBuilder queryBuilder;
    private CassandraQueryBuilderDirector queryDirector;

    int USOLevel;
    private static final String[] unsupportedServerOperators = {"OperatorNotEqual", "OperatorInGeoCircle", "OperatorInGeoPolygon", "OperatorInGeoRectangle",
            "OperatorInGeoTemporalCircle", "OperatorInGeoTemporalPolygon", "OperatorInGeoTemporalRectangle", "OperatorInGeoTextualCircle", "OperatorInGeoTextualPolygon",
            "OperatorInGeoTextualRectangle", "OperatorAllKeywords", "OperatorAnyKeywords"};


    private CassandraOperators(NoSqlDbConnector noSqlDbConnector, String dataCollection, SparkSession sparkSession) {
        super(noSqlDbConnector, dataCollection, sparkSession);
        selectClauseList = new ArrayList[2];
        selectClauseList[0] = new ArrayList<>(); // Project list
        selectClauseList[1] = new ArrayList<>(); // Aggregation list
        selectClauseListUSO = new ArrayList<>();
        selectClauseListGeo = new ArrayList<>();
        whereClauseList = new ArrayList<>();
        groupByClauseList = new ArrayList<>();
        orderByClauseList = new ArrayList<>();
        limit = -1;
        queryBuilder = new CassandraQueryBuilder();
        queryDirector = new CassandraQueryBuilderDirector(queryBuilder, cassandraConnectionManager.getConnection(getNoSqlDbConnector()).getKeyspace().get().asCql(true), getDataCollection());
    }

    static CassandraOperators newCassandraOperators(NoSqlDbConnector noSqlDbConnector, String dataCollection, SparkSession sparkSession) {
        return new CassandraOperators(noSqlDbConnector, dataCollection, sparkSession);
    }

    @Override
    public NoSqlDbOperators filter(FilterOperator filterOperator, FilterOperator... filterOperators) {
        ArrayList<FilterOperator> fops = new ArrayList<>();
        fops.add(filterOperator);
        fops.addAll(Arrays.asList(filterOperators));
        while (!fops.isEmpty()) {
            FilterOperator fop = fops.remove(0);
            if (fop.getClass().getSimpleName().equals("OperatorAnd")) {
                fops.addAll((ArrayList<FilterOperator>) fop.getOperatorExpression());
            } else if (Arrays.asList(unsupportedServerOperators).contains(fop.getClass().getSimpleName())) { // If the operator is not unsupported by the server
                if (fop instanceof GeographicalOperator) {
                    if (!(fop instanceof OperatorInGeoRectangle)) {
                        String[] operation = (String[]) fop.getOperatorExpression();
                        selectClauseListUSO.add(operation[0]);
                        whereClauseList.add(operation[1]);
                    } else {
                        String operation = (String) fop.getOperatorExpression();
                        whereClauseList.add(operation);
                    }
                    ((GeographicalOperator) fop).createIndex(getDataCollection(), cassandraConnectionManager.getConnection(getNoSqlDbConnector()));
                } else if (fop instanceof GeoTemporalOperator) {
                    String[] operation = (String[]) fop.getOperatorExpression();
                    if (!(fop instanceof OperatorInGeoTemporalRectangle)) {
                        selectClauseListUSO.add(operation[0]);
                        whereClauseList.add(operation[1]);
                        whereClauseList.add(operation[2]);
                    } else {
                        whereClauseList.add(operation[0]);
                        whereClauseList.add(operation[1]);
                    }
                    ((GeoTemporalOperator) fop).createIndex(getDataCollection(), cassandraConnectionManager.getConnection(getNoSqlDbConnector()));
                } else if (fop instanceof GeoTextualConstraintOperator) {
                    String[] operation = (String[]) fop.getOperatorExpression();
                    if (!(fop instanceof OperatorInGeoTextualRectangle)) {
                        selectClauseListUSO.add(operation[0]);
                        whereClauseList.add(operation[1]);
                        selectClauseListUSO.add(operation[2]);
                    } else {
                        whereClauseList.add(operation[0]);
                        selectClauseListUSO.add(operation[1]);
                    }
                    ((GeoTextualConstraintOperator<Geometry>) fop).createIndex(getDataCollection(), cassandraConnectionManager.getConnection(getNoSqlDbConnector()));
                } else {
                    String operation = fop.getOperatorExpression().toString();
                    selectClauseListUSO.add(operation);
                }
            } else { // If the operator is unsupported by the server
                whereClauseList.add(fop.getOperatorExpression().toString());
            }
            whereClauseList = (ArrayList<String>) whereClauseList.stream().distinct().collect(Collectors.toList());
        }
        return this;
    }

    @Override
    public NoSqlDbOperators groupBy(String fieldName, String... fieldNames) {
        expandList(fieldName, fieldNames, groupByClauseList);
        return this;
    }

    @Override
    public NoSqlDbOperators aggregate(AggregateOperator aggregateOperator, AggregateOperator... aggregateOperators) {
        expandList(aggregateOperator, aggregateOperators, selectClauseList[1]);
        return this;
    }

    @Override
    public NoSqlDbOperators distinct(String fieldName) {
        throw new UnsupportedOperationException("The distinct operator is not supported by Cassandra");
    }

    @Override
    public void printScreen() {
        ResultSet results = createResults();
        System.out.println("THE RESULTS ARE :");
        results.forEach(row -> System.out.println(row.getFormattedContents()));
    }


    @Override
    public Optional<Double> max(String fieldName) {
        String agrFieldName = fieldName + "_max";
        selectClauseList[1].add("MAX(" + fieldName + ") AS " + agrFieldName);
        ResultSet results = createResults();
        return Optional.of(Double.valueOf(results.one().getObject(agrFieldName).toString()));
    }

    @Override
    public Optional<Double> min(String fieldName) {
        String agrFieldName = fieldName + "_min";
        selectClauseList[1].add("MIN(" + fieldName + ") AS " + agrFieldName);
        ResultSet results = createResults();
        return Optional.of(Double.valueOf(results.one().getObject(agrFieldName).toString()));
    }

    @Override
    public Optional<Double> sum(String fieldName) {
        String agrFieldName = fieldName + "_sum";
        selectClauseList[1].add("SUM(" + fieldName + ") AS " + agrFieldName);
        ResultSet results = createResults();
        return Optional.of(Double.valueOf(results.one().getObject(agrFieldName).toString()));
    }

    @Override
    public Optional<Double> avg(String fieldName) {
        String agrFieldName = fieldName + "_avg";
        selectClauseList[1].add("AVG(" + fieldName + ") AS " + agrFieldName);
        ResultSet results = createResults();
        return Optional.of(Double.valueOf(results.one().getObject(agrFieldName).toString()));
    }

    @Override
    public int count() {
        selectClauseList[1].add("COUNT(*)");
        ResultSet results = createResults();
        return Integer.valueOf(results.one().getObject(0).toString());
    }

    @Override
    public NoSqlDbOperators sort(SortOperator sortOperator, SortOperator... sortingOperators) {
        expandList(sortOperator, sortingOperators, orderByClauseList);
        return this;
    }

    @Override
    public NoSqlDbOperators limit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public NoSqlDbOperators project(String fieldName, String... fieldNames) {
        expandList(fieldName, fieldNames, selectClauseList[0]);
        return this;
    }

    @Override
    public Dataset<Row> toDataframe() {
        return null;
    }

    @Override
    public NoSqlDbOperators join(NoSqlDbOperators noSqlDbOperators, JoinOperator jo) {
        return null;
    }

    @Override
    public NoSqlDbResults getResults() {
        ResultSet results = createResults();
        return new CassandraDBResults(results);
    }

    private static void expandList(String fieldName, String[] fieldNames, ArrayList<String> cqlClauseList) {
        String[] fields = new String[fieldNames.length + 1];
        fields[0] = fieldName;
        System.arraycopy(fieldNames, 0, fields, 1, fieldNames.length);
        cqlClauseList.addAll(Arrays.asList(fields));
    }

    private static void expandList(Operator operator, Operator[] operators, ArrayList<String> cqlClauseList) {
        Operator[] ops = new Operator[operators.length + 1];
        ops[0] = operator;
        System.arraycopy(operators, 0, ops, 1, operators.length);
        cqlClauseList.addAll(Arrays.asList(ops).stream().map(op -> op.getOperatorExpression().toString()).collect(Collectors.toList()));
    }

    private ResultSet createResults() {
        ResultSet results = null;
        if (selectClauseListUSO.isEmpty() && selectClauseListGeo.isEmpty()) {  // No USO operators
            String query = queryDirector.makeFullSelectQuery(selectClauseList[0], selectClauseList[1], whereClauseList, groupByClauseList, orderByClauseList, limit);
            results = cassandraConnectionManager.getConnection(getNoSqlDbConnector()).execute(query);
        } else { // USO operators
            String USOQuery = queryDirector.makeUsoQuery(selectClauseListUSO, whereClauseList, cassandraConnectionManager.getConnection(getNoSqlDbConnector()));
            ResultSet USOResults = cassandraConnectionManager.getConnection(getNoSqlDbConnector()).execute(USOQuery);
            results = postProcessResults(USOResults);
        }
        return results;
    }

    private ResultSet postProcessResults(ResultSet results) {
        ResultSet postProcessedResults = null;
        TableMetadata tableMetadata = cassandraConnectionManager.getConnection(getNoSqlDbConnector()).getMetadata().getKeyspace(cassandraConnectionManager.getConnection(getNoSqlDbConnector()).getKeyspace().get().asCql(true)).get().getTable(getDataCollection()).get();
        Map<CqlIdentifier, ColumnMetadata> columns = tableMetadata.getColumns();
        List<ColumnMetadata> partitionKeys = tableMetadata.getPartitionKey();
        Map<ColumnMetadata, ClusteringOrder> clusteringKeys = tableMetadata.getClusteringColumns();
        String createFilteredTableQuery = queryDirector.makeFilteredTableQuery(columns, partitionKeys, clusteringKeys);
        cassandraConnectionManager.getConnection(getNoSqlDbConnector()).execute(createFilteredTableQuery);
        filterRows(results, tableMetadata.getColumns());
        postProcessedResults = applyRestOfOperation();
        return postProcessedResults;
    }

    private void filterRows(ResultSet USOResults, Map<CqlIdentifier, ColumnMetadata> columns) {
        Supplier<IntStream> USOIndexes = () -> IntStream.range(columns.size(), columns.size() + selectClauseListUSO.size() + selectClauseListGeo.size());
        USOResults.forEach(row -> {
            if (USOIndexes.get().allMatch(row::getBoolean)) {
                String insertQuery = queryDirector.makeInsertQuery(columns, row);
                cassandraConnectionManager.getConnection(getNoSqlDbConnector()).execute(insertQuery);
            }
        });
    }

    private ResultSet applyRestOfOperation() {
        String operationQuery = queryDirector.makeApplyRestOfOperationsQuery(selectClauseList[0], selectClauseList[1], whereClauseList, groupByClauseList, orderByClauseList, limit);
        return cassandraConnectionManager.getConnection(getNoSqlDbConnector()).execute(operationQuery);
    }

}
