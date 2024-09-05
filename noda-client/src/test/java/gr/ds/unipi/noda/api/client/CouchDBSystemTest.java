package gr.ds.unipi.noda.api.client;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbOperators;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbResults;
import gr.ds.unipi.noda.api.core.operators.filterOperators.geoperators.Coordinates;
import gr.ds.unipi.noda.api.couchdb.aggregateOperators.OperatorCountDistinct;
import org.junit.Ignore;
import org.junit.Test;

import static gr.ds.unipi.noda.api.core.operators.AggregateOperators.avg;
import static gr.ds.unipi.noda.api.core.operators.FilterOperators.*;
import static gr.ds.unipi.noda.api.core.operators.SortOperators.desc;

public class CouchDBSystemTest {
    private NoSqlDbSystem getSystem() {
        return NoSqlDbSystem.CouchDB()
                .Builder("admin", "password")
                .host("localhost")
                .port(5984)
                .readTimeout(500)
                .connectTimeout(500)
                .build();
    }

    @Ignore
    @Test
    public void couchdbTest() {
        getSystem().operateOn("people")
                .aggregate(OperatorCountDistinct.newOperatorCountDistinct("favoriteFruit"))
                .printScreen();
    }

    @Ignore
    @Test
    public void simpleFilterTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("people").filter(gte("age", 30)).printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Ignore
    @Test
    public void simpleGroupByTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("people").groupBy("eyeColor").printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Ignore
    @Test
    public void groupByCombinedWithAggregationTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        NoSqlDbResults<?> res = noSqlDbSystem.operateOn("people")
                .filter(or(eq("age", 18), eq("age", 20)))
                .groupBy("eyeColor", "isActive")
                .aggregate(avg("age"))
                .getResults();
        while (res.hasNextRecord()) {
            System.out.println(res.getRecord().toString());
        }
        noSqlDbSystem.closeConnection();
    }

    @Ignore
    @Test
    public void logicalOperatorTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("people").filter(and(gte("age", 22), lte("age", 40))).printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Ignore
    @Test
    public void textualOperatorsTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        noSqlDbSystem.operateOn("people")
                // .filter(allKeywords("tags", "dolor", "nulla"))
                .filter(anyKeywords("tags", "dolor", "nulla")).sort(desc("age")).printScreen();
        noSqlDbSystem.closeConnection();
    }

    @Ignore
    @Test
    public void structuralSharingTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("people");

        NoSqlDbOperators var = noSqlDbOperators.filter(and(gte("weight", 200), lte("weight", 500)));
        var = noSqlDbOperators.groupBy("age").project("name");
        var.printScreen();

        noSqlDbSystem.closeConnection();
    }

    @Ignore
    @Test
    public void geometryTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();
        NoSqlDbOperators noSqlDbOperators = noSqlDbSystem.operateOn("people");

        noSqlDbOperators.filter(inGeoTextualCircleKm("location",
                Coordinates.newCoordinates(23.622549, 37.94739),
                100,
                anyKeywords("tags", "nulla")
        )).printScreen();
    }

    @Ignore
    @Test
    public void getResultsTest() {
        NoSqlDbSystem noSqlDbSystem = getSystem();

        NoSqlDbResults<?> results = noSqlDbSystem.operateOn("people").filter(gte("age", 10)).getResults();

        while (results.hasNextRecord()) {
            System.out.println(results.getRecord());
        }

        noSqlDbSystem.closeConnection();
    }
}
