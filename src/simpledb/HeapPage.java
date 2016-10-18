package simpledb;

import java.util.*;
import java.io.*;

/**
 * HeapPage stores pages of HeapFiles and implements the Page interface that
 * is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    HeapPageId pid;
    TupleDesc td;
    int header[];
    Tuple tuples[];
    int numSlots;
    boolean dirty;			// dirty_bit
    int pin_count;			// pin_count
    TransactionId lasttrans;


    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of 32-bit header words indicating
     * the slots of the page that are in use, plus (BufferPool.PAGE_SIZE/tuple
     * size) tuple slots, where tuple size is the size of tuples in this
     * database tables, which can be determined via {@link Catalog#getTupleDesc}.
     *
     * The number of 32-bit header words is equal to:
     * <p>
     * (no. tuple slots / 32) + 1
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#PAGE_SIZE
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.tableid());
        //this.numSlots = (BufferPool.PAGE_SIZE) / (td.getSize());
        this.numSlots = (BufferPool.PAGE_SIZE*8) / ((td.getSize()*8)+1);
		//System.out.println(this.numSlots);
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
		header = new int[(numSlots/32)+1];
        for (int i=0; i<header.length; i++){
	       header[i] = dis.readInt();
	       //System.out.println("HEADER READ["+i+"]="+header[i]);
		}
        try{
            // allocate and read the actual records of this page
            tuples = new Tuple[numSlots];
            for (int i=0; i<numSlots; i++){
                tuples[i] = readNextTuple(dis,i);
	    }
        }catch(NoSuchElementException e){
            //e.printStackTrace();
        }
        dis.close();
        
        // initialize pin_count and dirty_bit
        this.pin_count = 0;
        this.dirty = false;

    }

    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        // do not need to implement this
      
        return null;
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId id() {
	return this.pid;
   
    }
    
    public int pin_count() {
    	return this.pin_count;
    }
    

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!getSlot(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                } 
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordID rid = new RecordID(pid, slotId);
        t.setRecordID(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            //e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
	// int len = header.length*4 + BufferPool.PAGE_SIZE;
	int len = BufferPool.PAGE_SIZE;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeInt(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<numSlots; i++) {

            // empty slot
            if (!getSlot(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.PAGE_SIZE - numSlots * td.getSize() -header.length*4;
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @param tableid The id of the table that this empty page will belong to.
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData(int tableid) {
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
	// int hb = (((BufferPool.PAGE_SIZE / td.getSize()) / 32) +1) * 4;
        int len = BufferPool.PAGE_SIZE;// + hb;
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public boolean deleteTuple(Tuple t) throws DbException {
        // no need to implement this
        return false;
    }

    /**
     * Adds the specified tuple to the page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void addTuple(Tuple t) throws DbException {
        // no need to implement this
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        this.dirty=dirty;
	this.lasttrans=tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        if (this.dirty==true)
	    return this.lasttrans;
	else
	    return null;
    }
    
    /**
     * Increment pin_count (pinning). It happens every time a page is requested but not released.
     */    
    public void pin() {
    	this.pin_count++;
    }
    
    /**
     * Decrement pin_count (unpinning). It happens when the page is released.
     */        
    public void unpin() {
    	this.pin_count--;
    }

    /**
     * Returns the number of empty slots on this page.
     * by Romina Nayak
     */
    public int getNumEmptySlots() {
        int emptySlots = 0;
        for (int i=0; i<this.numSlots ; i++) {
            if(!getSlot(i)) {
                emptySlots++;
            }
        }
    	return emptySlots;
    }
    
    /**
     * Returns true if associated slot on this page is filled.
     * by Romina Nayak
     */
    public boolean getSlot(int i) {
        int headerVal = i / 32; //position of 32-bit integer
        int bitPos = i % 32; //position of slot bit in the integer
        int h = this.header[headerVal];
        int value = (h & (byte) 1 << bitPos);
        if (value == 0) {
            return false;
        } else {
	    return true;
        }
    }
	
    /**
     * Abstraction to fill a slot on this page.
     * by Romina Nayak
     */
    private void setSlot(int i, boolean value) {
        int headerVal = (int)Math.floor(i / 32);
        int rem = i % 32;
        if (value) {
            header[headerVal] = header[headerVal] | (int)Math.pow(2, rem-1);
        } else {
            header[headerVal] = header[headerVal] & ~(int)Math.pow(2, rem-1);
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new HeapPageIterator(this);
    }

    public int getNumSlots() {
	return this.numSlots;
    }



}

