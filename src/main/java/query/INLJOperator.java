package query;

import database.Database;
import database.DatabaseException;
import table.Record;
import databox.DataBox;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.List;

public class INLJOperator extends JoinOperator {

    public INLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        Database.Transaction transaction) throws QueryPlanException {
        super(leftSource,
                rightSource,
                leftColumnName,
                rightColumnName,
                transaction,
                JoinType.INLJ);

    }

    @Override
    public Iterator<Record> iterator() throws QueryPlanException, DatabaseException {
        //throw new UnsupportedOperationException("TODO: implement");
        return new INLJIterator();
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     * Note that the left table is the "outer" loop and the right table is the "inner" loop.
     */
    private class INLJIterator implements Iterator<Record> {
        private Iterator<Record> leftIterator;
        private Iterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;

        public INLJIterator() throws QueryPlanException, DatabaseException {
            this.leftIterator = INLJOperator.this.getLeftSource().iterator();
            this.rightIterator = null;
            this.leftRecord = null;
            this.nextRecord = null;
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        public boolean hasNext() {
            if (this.nextRecord != null) {
                return true;
            }
            while (true) {
                if (this.leftRecord == null) {
                    if (this.leftIterator.hasNext()) {
                        this.leftRecord = this.leftIterator.next();
                        try {
                            this.rightIterator = INLJOperator.this.getRightSource().iterator();
                        } catch (QueryPlanException q) {
                            return false;
                        } catch (DatabaseException e) {
                            return false;
                        }
                    } else {
                        return false;
                    }
                }
                while (this.rightIterator.hasNext()) {
                    Record rightRecord = this.rightIterator.next();
                    DataBox leftJoinValue = this.leftRecord.getValues().get(INLJOperator.this.getLeftColumnIndex());
                    DataBox rightJoinValue = rightRecord.getValues().get(INLJOperator.this.getRightColumnIndex());
                    if (leftJoinValue.equals(rightJoinValue)) {
                        List<DataBox> leftValues = new ArrayList<DataBox>(this.leftRecord.getValues());
                        List<DataBox> rightValues = new ArrayList<DataBox>(rightRecord.getValues());
                        leftValues.addAll(rightValues);
                        this.nextRecord = new Record(leftValues);
                        return true;
                    }
                }
                this.leftRecord = null;
            }
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        public Record next() {
            if (this.hasNext()) {
                Record r = this.nextRecord;
                this.nextRecord = null;
                return r;
            }
            throw new NoSuchElementException();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }


}
