package simpledb;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;
    private Map<Field, Integer> count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what != Op.COUNT) {
          throw new IllegalArgumentException();
        }
        td = buildTupleDesc();
        count = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupfield = null;
        if (gbfieldtype != null) {
          groupfield = tup.getField(gbfield);
        }
        if (count.containsKey(groupfield)) {
          count.put(groupfield, count.get(groupfield) + 1);
        } else {
          count.put(groupfield, 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
      Set<Field> fields = count.keySet();
      Iterator<Field> fieldsIter = fields.iterator();
      List<Tuple> tupleList = new ArrayList<Tuple>();
      while(fieldsIter.hasNext()){
        Tuple tuple = new Tuple(td);
        Field key = (Field)fieldsIter.next();
        if(gbfield == Aggregator.NO_GROUPING){
          tuple.setField(0, new IntField(count.get(key)));
        }else{
          tuple.setField(0, key);
          tuple.setField(1, new IntField(count.get(key)));
        }
        tupleList.add(tuple);
      }
      return new TupleIterator(td, tupleList);
    }
    
    private TupleDesc buildTupleDesc() {
      if (gbfieldtype == null) {
        Type[] typeAr = new Type[]{Type.STRING_TYPE};
        return new TupleDesc(typeAr);
      } else {
        Type[] typeAr = new Type[]{gbfieldtype, Type.INT_TYPE};
        return new TupleDesc(typeAr);
      }
    }
}
