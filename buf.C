#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}

//TODO: Soft Liampisan
const Status BufMgr::allocBuf(int & frame) 
{
   int iterations = 0; 
    do {
        //Advance clock pointer
        clockHand = (clockHand + 1) % numBufs;
        //Check valid bit
        if (bufTable[clockHand].valid) { 
            //Check refBit
            if (bufTable[clockHand].refbit) { 
                bufTable[clockHand].refbit = false;
                continue;
            } else { 
                if (bufTable[clockHand].pinCnt > 0) { 
                    continue;
                }
                //Page is not pinned, remove from hashtable first
                hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);

                //Check dirty bit
                if (bufTable[clockHand].dirty) { 
                    // Flush page to disk
                    if (bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &(bufPool[clockHand])) != OK) {
                        return UNIXERR;
                    }
                    bufTable[clockHand].dirty = false; 
                }
                frame = clockHand;
                bufTable[clockHand].Clear(); 
                return OK;
            }
            //valid bit not set
        } else { 
            frame = clockHand;
            bufTable[clockHand].Clear(); 
            return OK;
        }
        iterations++;
    } while (iterations < numBufs * 2);
    //All buffer frames are pinned, return BUFFEREXCEEDED
    return BUFFEREXCEEDED;

}

//TODO: Nott Laoaroon
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{





}

//TODO: Nott Laoaroon
const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{





}

//TODO: Kevin Zhao
const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{

    int pageNum, frameNum;
    Status stat;
    Error err;

    stat = file->allocatePage(pageNum);

    if (stat != OK) {
        cout << "BufMgr::allocPage(), file->allocatePage()";
        err.print(stat);
        return stat;
    }

    stat = allocBuf(frameNum);

    if (stat != OK) {
        cout << "BufMgr::allocPage(), allocBuf()";
        err.print(stat);
        return stat;
    }

    stat = hashTable->insert(file, pageNum, frameNum);

    if (stat != OK) {
        cout << "BufMgr::allocPage(), hashTable->insert()";
        err.print(stat);
        return stat;
    }

    bufTable->Set(file, pageNum);

    pageNo = pageNum;
    page = (bufPool + frameNum * (sizeof(Page)));

    return OK;

}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


