package simpledb;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc td;
  
    private Map<Field, Integer> count;
    private Map<Field, Integer> sum;
    private Map<Field, Integer> avg;
    private Map<Field, Integer> min;
    private Map<Field, Integer> max;

    private int groupCount;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.td = buildTupleDesc();

        this.count = new HashMap<Field, Integer>();
        this.sum = new HashMap<Field, Integer>();
        this.avg = new HashMap<Field, Integer>();
        this.min = new HashMap<Field, Integer>();
        this.max = new HashMap<Field, Integer>();

        this.groupCount = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int aggFieldValue = ((IntField)tup.getField(afield)).getValue();
        Field groupfield = null;
        if (gbfieldtype != null) {
          groupfield = tup.getField(gbfield);
        }
        if (count.containsKey(groupfield)) {
          groupCount += 1;
          count.put(groupfield, count.get(groupfield) + 1);
          sum.put(groupfield, sum.get(groupfield) + aggFieldValue);
          avg.put(groupfield, sum.get(groupfield) / groupCount);
          min.put(groupfield, Math.min(min.get(groupfield), aggFieldValue));
          max.put(groupfield, Math.max(max.get(groupfield), aggFieldValue));
        } else {
          groupCount = 1;
          count.put(groupfield, 1);
          sum.put(groupfield, aggFieldValue);
          avg.put(groupfield, aggFieldValue);
          min.put(groupfield, aggFieldValue);
          max.put(groupfield, aggFieldValue);
        }
    }


    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
      switch (what) {
        case COUNT:
          return getTupleIterator(count);
        case SUM:
          return getTupleIterator(sum);
        case AVG:
          return getTupleIterator(avg);
        case MIN:
          return getTupleIterator(min);
        case MAX:
          return getTupleIterator(max);
        default:
          return null;
      }
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

    private DbIterator getTupleIterator(Map<Field, Integer> map) {
      Set<Field> fields = map.keySet();
      Iterator<Field> fieldsIter = fields.iterator();
      List<Tuple> tupleList = new ArrayList<Tuple>();
      while(fieldsIter.hasNext()){
        Tuple tuple = new Tuple(td);
        Field key = (Field)fieldsIter.next();
        if(gbfield == Aggregator.NO_GROUPING){
          tuple.setField(0, new IntField(map.get(key)));
        }else{
          tuple.setField(0, key);
          tuple.setField(1, new IntField(map.get(key)));
        }
        tupleList.add(tuple);
      }
      return new TupleIterator(td, tupleList); 
    }
}
