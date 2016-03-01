package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile data = new RandomAccessFile(file, "r");
            byte[] inData = new byte[BufferPool.PAGE_SIZE];
            if (pid.pageNumber() > data.length()/(float)BufferPool.PAGE_SIZE) {
                throw new IllegalArgumentException();
            }
            data.skipBytes(pid.pageNumber()*BufferPool.PAGE_SIZE);
            data.read(inData,0,BufferPool.PAGE_SIZE);
            data.close();
            HeapPage rtn = new HeapPage((HeapPageId)pid, inData);
            return rtn;
        } catch (IOException e) {
            return null;
        }
        
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length()/(long)BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        final TransactionId tranId = tid;
        
        DbFileIterator it = new DbFileIterator() {
            BufferPool bufferPool = Database.getBufferPool();
            Iterator<Tuple> pageitr;
            int pageNumber = 0;
            int hasOpen = 0;
              /**
             * Opens the iterator
             * @throws DbException when there are problems opening/accessing the database.
             */
            @Override
            public void open()
                throws DbException, TransactionAbortedException {
                PageId pid = new HeapPageId(getId(), 0);
                Page page = bufferPool.getPage(tranId, pid, Permissions.READ_ONLY);
                pageitr = ((HeapPage) page).iterator();
                hasOpen = 1;
            }

            /** @return true if there are more tuples available. */
            @Override
            public boolean hasNext()
                throws DbException, TransactionAbortedException{
                if (hasOpen == 1) {
                    if (pageNumber >= (file.length()/BufferPool.PAGE_SIZE)-1 && !pageitr.hasNext()) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
            /**
             * Gets the next tuple from the operator (typically implementing by reading
             * from a child operator or an access method).
             *
             * @return The next tuple in the iterator.
             * @throws NoSuchElementException if there are no more tuples
             */
            @Override
            public Tuple next()
                throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                if (pageitr.hasNext()) {
                    return pageitr.next();
                } else if (pageNumber == file.length()/BufferPool.PAGE_SIZE-1) { 
                    throw new NoSuchElementException();
                } else {
                    pageNumber++;
                    PageId pid = new HeapPageId(getId(), pageNumber);
                    Page page = bufferPool.getPage(tranId, pid, Permissions.READ_ONLY);
                    pageitr = ((HeapPage) page).iterator();
                    return pageitr.next();
                }
            }

            /**
             * Resets the iterator to the start.
             * @throws DbException When rewind is unsupported.
             */
            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.open();
            }

            @Override
            public void close(){
                hasOpen = 0;
                Iterator<Tuple> pageitr = null;
                int pageNumber = 0;
            }
        };
        return it;
    }
      

}

