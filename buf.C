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


const Status BufMgr::allocBuf(int & frame) 
{ 
    int pagesPinned = 0;
    bool pinned[numBufs] = {0};
    while(1){
        if(pagesPinned == numBufs){
            return Status::BUFFEREXCEEDED;
        }
        advanceClock();
        if(!bufTable[clockHand].valid){
            break;
        }
        if(bufTable[clockHand].refbit){
            bufTable[clockHand].refbit = 0;
            continue;
        }
        if(bufTable[clockHand].pinCnt){
            if(!pinned[clockHand]) pagesPinned++;
            pinned[clockHand] = 1;
            continue;
        }
        if(bufTable[clockHand].dirty){
            Status writeStatus = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, bufPool + clockHand);
            if(writeStatus != Status::OK){
                return writeStatus;
            }
        }
        break;
    }
    if(bufTable[clockHand].valid){
        Status removeStatus = hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
        if(removeStatus != Status::OK){
            return removeStatus;
        }
    }
    frame = clockHand;
    bufTable[clockHand].Clear();
    return Status::OK;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo = -1;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if(status == Status::HASHNOTFOUND){
        Status allocStatus = allocBuf(frameNo);
        if(allocStatus != Status::OK){
            return allocStatus;
        }
        Status readStatus = file->readPage(PageNo, bufPool + frameNo);
        if(readStatus != Status::OK){
            return readStatus;
        }
        Status insertStatus = hashTable->insert(file, PageNo, frameNo);
        if(insertStatus != Status::OK){
            return insertStatus;
        }
        bufTable[frameNo].Set(file, PageNo);
        page = bufPool + frameNo;
    }
    else{
        bufTable[frameNo].refbit = 1;
        bufTable[frameNo].pinCnt++;
        page = bufPool + frameNo;
    }
    return Status::OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo = -1;
    Status status = hashTable->lookup(file, PageNo, frameNo);
    if(status != Status::OK){
        return status;
    }
    if(bufTable[frameNo].pinCnt == 0){
        return Status::PAGENOTPINNED;
    }
    bufTable[frameNo].pinCnt--;
    if(dirty) bufTable[frameNo].dirty = 1;
    return Status::OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    int newPageNo = -1;
    Status allocStatus = file->allocatePage(newPageNo);
    if(allocStatus != Status::OK){
        return allocStatus;
    }
    int frameNo = -1;
    Status allocBufStatus = allocBuf(frameNo);
    if(allocBufStatus != Status::OK){
        return allocBufStatus;
    }
    Status insertStatus = hashTable->insert(file, newPageNo, frameNo);
    if(insertStatus != Status::OK){
        return insertStatus;
    }
    bufTable[frameNo].Set(file, newPageNo);
    pageNo = newPageNo;
    page = bufPool + frameNo;
    return Status::OK;
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


