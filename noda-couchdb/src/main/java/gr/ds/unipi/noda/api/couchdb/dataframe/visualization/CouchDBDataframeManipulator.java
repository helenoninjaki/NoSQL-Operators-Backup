package gr.ds.unipi.noda.api.couchdb.dataframe.visualization;

import gr.ds.unipi.noda.api.core.dataframe.visualization.BaseDataframeManipulator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CouchDBDataframeManipulator extends BaseDataframeManipulator {
    @Override
    public Dataset<Row> spatialView(Dataset<Row> dataset, String location) {
        return null;
    }

    @Override
    public Dataset<Row> trajectoriesTimelapse(Dataset<Row> dataset, String location, String time) {
        return null;
    }
}
