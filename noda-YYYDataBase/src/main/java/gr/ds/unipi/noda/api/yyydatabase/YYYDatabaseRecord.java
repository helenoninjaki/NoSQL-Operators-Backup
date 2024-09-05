package gr.ds.unipi.noda.api.yyydatabase;

import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbRecord;
import gr.ds.unipi.noda.api.core.nosqldb.NoSqlDbResults;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;

public class YYYDatabaseRecord extends NoSqlDbRecord<Object> {

    protected YYYDatabaseRecord(Object objectRecord) {
        super(objectRecord);
    }

    @Override
    public boolean containsField(String field) {
        return false;
    }

    @Override
    public <U> boolean containsValue(U value) {
        return false;
    }

    @Override
    public boolean getBoolean(String field) {
        return false;
    }

    @Override
    public String getString(String field) {
        return null;
    }

    @Override
    public int getInt(String field) {
        return 0;
    }

    @Override
    public long getLong(String field) {
        return 0;
    }

    @Override
    public float getFloat(String field) {
        return 0;
    }

    @Override
    public double getDouble(String field) {
        return 0;
    }

    @Override
    public Date getDate(String field) {
        return null;
    }

    @Override
    public short getShort(String field) {
        return 0;
    }

    @Override
    public BigDecimal getBigDecimal(String field) {
        return null;
    }

    @Override
    public byte getByte(String field) {
        return 0;
    }

    @Override
    public <U> List<U> getList(String field, Class<U> clazz) {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }
}
