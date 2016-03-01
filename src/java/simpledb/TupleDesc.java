package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        private final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }

        public int hashCode() {
          final int prime = 31;
          int result = 1;
          result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
          result = prime * result + ((fieldType == null) ? 0 : fieldType.hashCode());
          return result;
        }

        @Override
        public boolean equals(Object obj) {
          if (this == obj)
            return true;
          if (obj == null)
            return false;
          if (getClass() != obj.getClass())
            return false;
          TDItem other = (TDItem) obj;
          if (fieldName == null) {
            if (other.fieldName != null)
              return false;
          } else if (!fieldName.equals(other.fieldName))
            return false;
          if (fieldType != other.fieldType)
            return false;
          return true;
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    public List<TDItem> tdItems = new LinkedList<TDItem>();

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        for (int i = 0; i < typeAr.length; i++) {
          TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
          tdItems.add(tdItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
          throw new NoSuchElementException();
        }
        return tdItems.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        if (i < 0 || i >= numFields()) {
          throw new NoSuchElementException();
        }
        return tdItems.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < tdItems.size(); i++) {
          String fName = tdItems.get(i).fieldName;
          if (fName!= null && fName.equals(name)) {
            return i;
          }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < tdItems.size(); i++) {
          size += tdItems.get(i).fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int size1 = td1.numFields();
        int size2 = td2.numFields();
        Type[] td = new Type[size1 + size2];
        String[] name = new String[size1 + size2];
        for (int i = 0; i < size1; i++) {
          td[i] = td1.getFieldType(i);
          name[i] = td1.getFieldName(i);
        }
        for (int i = 0; i < size2; i++) {
          td[i + size1] = td2.getFieldType(i);
          name[i + size1] = td2.getFieldName(i);
        }
        return new TupleDesc(td, name);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param obj
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object obj) {
        // some code goes here
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      TupleDesc other = (TupleDesc) obj;
      if (tdItems == null) {
        if (other.tdItems != null)
          return false;
      } else {
        int nItems = tdItems.size();
        if (other.tdItems.size() != nItems)
          return false;
        for (int i = 0; i < nItems; i++) {
          if (tdItems.get(i).fieldType != other.tdItems.get(i).fieldType)
            return false;
        }
      }
      return true; 
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        int result = 1;
        result = 31 * result + ((tdItems == null) ? 0 : tdItems.hashCode());
        return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < numFields(); i++) {
          buffer.append(tdItems.get(i).fieldType.toString() + "(");
          buffer.append(tdItems.get(i).fieldName + "),");
        }
        return buffer.toString().substring(0, buffer.length() - 1);
    }
}
