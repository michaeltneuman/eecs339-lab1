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
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }
    
    private TDItem[] fields;

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	Iterator<TDItem> iter = new Iterator<TDItem>() {
    		int index = 0;
    		public boolean hasNext() {
    			return (index < fields.length);
    		}
    		public TDItem next() {
    			return fields[index + 1];
    		}
    	};
    	return iter;
    }

    private static final long serialVersionUID = 1L;

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
        fields = new TDItem[typeAr.length];
    	for (int index = 0; index < typeAr.length; index++) {
        	fields[index] = new TDItem(typeAr[index],fieldAr[index]);
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
        if (typeAr.length < 1) {
        	throw new IllegalArgumentException("must contain at least one entry");
        }
        fields = new TDItem[typeAr.length];
        for (int index = 0; index < typeAr.length; index++) {
        	fields[index] = new TDItem(typeAr[index],null);
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return fields.length;
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
        if (i < 0 || i > fields.length || fields[i].fieldName == null) {
        	throw new NoSuchElementException("i is not a valid field reference");
        }
        return fields[i].fieldName;
        	
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
        if (i < 0 || i > fields.length) {
        	throw new NoSuchElementException("i is not a valid field reference");
        }
        return fields[i].fieldType;
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
    	for (int index = 0; index < fields.length; index++) {
    		String fname = getFieldName(index);
    		if (!fname.equals(null) && fname.equals(name)) {
    			return index;	
        	}
        }
        throw new NoSuchElementException("no field with a matching name is found");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (int index = 0; index < fields.length; index++) {
        	size = size + fields[index].fieldType.getLen();
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
        int newlength = td1.fields.length + td2.fields.length;
        String[] names = new String[newlength];
        Type[] types = new Type[newlength];
        for (int index = 0; index < newlength; index++) {
        	if (index < td1.fields.length) {
        		names[index] = td1.fields[index].fieldName;
        		types[index] = td1.fields[index].fieldType;
        	} else {
        		names[index] = td2.fields[index - td1.fields.length].fieldName;
        		types[index] = td2.fields[index - td1.fields.length].fieldType;
        	}
        }
        TupleDesc ret = new TupleDesc(types,names);
        return ret;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
    	if (o == null || o.getClass() != this.getClass()) {
    		return false;
    	}
        if (fields.length != ((TupleDesc) o).fields.length) {
        	return false;
        }
        for (int index = 0; index < fields.length; index++) {
        	if (fields[index].fieldType != ((TupleDesc) o).fields[index].fieldType) {
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String ret = "";
        for (TDItem item : fields) {
        	ret = item.fieldType + "(" + item.fieldName + ") ";
        }
        return ret;
    }
}
