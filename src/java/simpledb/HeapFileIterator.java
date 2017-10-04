package simpledb;

import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    	private int index;
    	private TransactionId trans_id;
    	private HeapFile file;
    	private Iterator<Tuple> iter;
    	
    	public HeapFileIterator(TransactionId tid, HeapFile f) {
    		this.trans_id = tid;
    		this.file = f;
    		this.iter = null;
    	}
    	
    	public void open() throws DbException, TransactionAbortedException {
    		index = 0;
    		PageId page_id = new HeapPageId(file.getId(),index);
    		Page p = Database.getBufferPool().getPage(trans_id, page_id,Permissions.READ_ONLY);
    		HeapPage heap_page = (HeapPage) p;
    		iter = heap_page.iterator();
    	}
    	
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		if (iter == null) {
				return false;
			}
    		if (iter.hasNext()) {
    			return true;
    		}
    		if (index >= file.numPages()-1) {
    			return false;
    		}
    		else {
    			PageId page_id = new HeapPageId(file.getId(),index++);
    			HeapPage heap_page = (HeapPage) Database.getBufferPool().getPage(trans_id,page_id,Permissions.READ_ONLY);
				return heap_page.iterator().hasNext();
    		}
    		
    	}

		public Tuple next() throws DbException, TransactionAbortedException,
				NoSuchElementException {
			if (iter == null) {
				throw new NoSuchElementException("no more tuples");
			}
			if(iter.hasNext()) {
				return iter.next();
			}
			else {
				PageId page_id = new HeapPageId(file.getId(), index+1);
				Page p = Database.getBufferPool().getPage(trans_id,page_id,Permissions.READ_ONLY);
    			HeapPage heap_page = (HeapPage) p;
    			if (heap_page.iterator().hasNext()) {
    				index++;
    				iter = heap_page.iterator();
    				return iter.next();
    			}
			}
			return null;
		}

		public void rewind() throws DbException, TransactionAbortedException {
			close();
			open();
		}

		public void close() {
			index = 0;
			iter = null;
		}

    }