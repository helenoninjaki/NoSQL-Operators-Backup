package gr.ds.unipi.noda.api.cassandra.aggregateOperators;

final class OperatorAvg extends AggregateOperator {

    private OperatorAvg(String fieldName) {
        super(fieldName, "avg_" + fieldName);
    }

    public static OperatorAvg newOperatorAvg(String fieldName) {
        return new OperatorAvg(fieldName);
    }

    public StringBuilder getOperatorExpression() {
        StringBuilder operation = new StringBuilder();
        operation.append("AVG(");
        operation.append(getFieldName());
        operation.append(")");
        return operation;
    }

}
