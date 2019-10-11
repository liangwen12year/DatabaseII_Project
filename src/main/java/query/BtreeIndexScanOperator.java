package query;

import database.Database;
import database.DatabaseException;
import databox.DataBox;
import index.BPlusTree;
import table.*;

import java.util.ArrayList;
import java.util.Iterator;

public class BtreeIndexScanOperator extends QueryOperator {
    private Database.Transaction transaction;
    private String tableName;
    //private DataBox value;
    private BPlusTree bPlusTree;


    public enum SearchType {
        EQUAL,
        GREATER,
        LESS,
        GREATER_OR_EQ,
        LESS_OR_EQ
    }

    //initialize the constructor
    public BtreeIndexScanOperator(Database.Transaction transaction,
                                  String tableName,
                                  BPlusTree bPlusTree) throws QueryPlanException, DatabaseException {
        super(OperatorType.INDEXSCAN);
        this.transaction = transaction;
        this.tableName = tableName;
        this.setOutputSchema(this.computeSchema());
        this.bPlusTree = bPlusTree;
        //this.stats = this.estimateStats();
        //this.cost = this.estimateIOCost();
    }

    @Override
    //use the getRecordIterator function to return the record
    public Iterator<Record> iterator() throws DatabaseException {
        return this.transaction.getRecordIterator(tableName);
    }

    @Override
    // use the getFullyQualifiedSchema function to compute schema
    public Schema computeSchema() throws QueryPlanException {
        try {
            return this.transaction.getFullyQualifiedSchema(this.tableName);
        } catch (DatabaseException de) {
            throw new QueryPlanException(de);
        }
    }

    @Override
    public Iterator<Record> execute(Object... arguments) throws QueryPlanException, DatabaseException {
        SearchType search_type = (SearchType)arguments[0];
        DataBox val = (DataBox) arguments[1];

        if (search_type == SearchType.EQUAL){
            return find_equal(val);
        }

        return null;
    }


    //use the scanEqual function of Bplustree to get the record ID
    // then use the RecordIterator function to convert the record ID to record and return.
    public Iterator<Record> find_equal(DataBox val) throws DatabaseException{
        //throw new UnsupportedOperationException("TODO: implement");
        Iterator<RecordId> recordidd = bPlusTree.scanEqual(val);

        Table t =this.transaction.getTable(tableName);

        return new RecordIterator(t, recordidd);

    }
}
