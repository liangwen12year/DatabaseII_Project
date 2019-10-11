package query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import testutil.TestUtils;
import table.Record;
import table.Schema;

public class TestSourceOperator extends QueryOperator {
  private List<Record> recordList;
  private Schema setSchema;
  private int numRecords;

  public TestSourceOperator() throws QueryPlanException {
    super(OperatorType.SEQSCAN, null);
    this.recordList = null;
    this.setSchema = null;
    this.numRecords = 100;
  }

  public TestSourceOperator(List<Record> recordIterator, Schema schema) throws QueryPlanException {
    super(OperatorType.SEQSCAN);

    this.recordList = recordIterator;
    this.setOutputSchema(schema);
    this.setSchema = schema;
    this.numRecords = 100;
  }

  public TestSourceOperator(List<Record> recordIterator, Schema schema, int numRecords) throws QueryPlanException {
    super(OperatorType.SEQSCAN);

    this.recordList = recordIterator;
    this.setOutputSchema(schema);
    this.setSchema = schema;
    this.numRecords = numRecords;
  }

  @Override
  public boolean isSequentialScan() {
    return false;
  }

  public TestSourceOperator(int numRecords) throws QueryPlanException {
    super(OperatorType.SEQSCAN, null);
    this.recordList = null;
    this.setSchema = null;
    this.numRecords = numRecords;
  }


  public Iterator<Record> execute() {
    if (this.recordList == null) {
      ArrayList<Record> recordList = new ArrayList<Record>();
      for (int i = 0; i < this.numRecords; i++) {
        recordList.add(TestUtils.createRecordWithAllTypes());
      }

      return recordList.iterator();
    }
    return this.recordList.iterator();
  }

  public Iterator<Record> iterator() {
    return this.execute();
  }

  protected Schema computeSchema() {
    if (this.setSchema == null) {
      return TestUtils.createSchemaWithAllTypes();
    }
    return this.setSchema;
  }

}
