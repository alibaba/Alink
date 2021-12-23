/* Copyright 2019 The flink-ai-extended Authors. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#ifndef SPSC_QUEUE_H
#define SPSC_QUEUE_H

#include <atomic>
#include <stdexcept>
#include <thread>
#include <cstring>
#include <string>
#include <sstream>
#include <iostream>
#include <assert.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

using byte = unsigned char;

#define ATOMIC_INT32_SIZE sizeof(uint32_t)
#define CACHE_LINE_SIZE 64

class SPSCQueueBase
{
  protected:
    byte *alignedRaw_;
    byte *arrayBase_;
    //consumer owns readPtr and writeCachePtr
    std::atomic<int64_t> *const readAtomicPtr_;
    int64_t *const readPtr_;
    int64_t *const writeCachePtr_;

    //producer owns writePtr and readCachePtr
    std::atomic<int64_t> *const writeAtomicPtr_;
    int64_t *const writePtr_;
    int64_t *const readCachePtr_;

    std::atomic<int64_t> *const finishAtomicPtr_;

    const int capacity_;
    const int mask_;

    const bool ipc_;
    const int mmapLen_;

    void dump() {
        printf("alignedRaw=%lld, arrayBase=%lld, readPtr=%lld, writeCachePtr=%lld, writePtr=%lld, readCachePtr=%lld, finishPtr=%lld, capacity=%d, mmapLen=%d\n",
        reinterpret_cast<int64_t>(alignedRaw_),
        reinterpret_cast<int64_t>(arrayBase_),
        reinterpret_cast<int64_t>(readPtr_),
        reinterpret_cast<int64_t>(writeCachePtr_),
        reinterpret_cast<int64_t>(writePtr_),
        reinterpret_cast<int64_t>(readCachePtr_),
        reinterpret_cast<int64_t>(finishAtomicPtr_),
         capacity_, mmapLen_);
    }

    /* Memory layout:
    |read----|writeCach|-------|---------|--------|--------|--------|--------|
    |capacity|--------|--------|---------|--------|--------|--------|--------|
    |write---|readCach|--------|---------|--------|--------|--------|--------|
    |finish--|--------|--------|---------|--------|--------|--------|--------|
    |........|........|........|.........|........|........|........|........|
    |........|........|........|.........|........|........|........|........|
    * */

  private:
    SPSCQueueBase(long alignedPtr, bool ipc, int64_t len) : alignedRaw_(reinterpret_cast<byte *>(alignedPtr)),
                                                   arrayBase_(reinterpret_cast<byte *>(alignedPtr + 4 * CACHE_LINE_SIZE)),

                                                   readAtomicPtr_(reinterpret_cast<std::atomic<int64_t> *>(alignedPtr)),
                                                   readPtr_(reinterpret_cast<int64_t *>(alignedPtr)),
                                                   writeCachePtr_(reinterpret_cast<int64_t *>(alignedPtr + 8)),

                                                   writeAtomicPtr_(reinterpret_cast<std::atomic<int64_t> *>(alignedPtr + 2 * CACHE_LINE_SIZE)),
                                                   writePtr_(reinterpret_cast<int64_t *>(alignedPtr + 2 * CACHE_LINE_SIZE)),
                                                   readCachePtr_(reinterpret_cast<int64_t *>(alignedPtr + 2 * CACHE_LINE_SIZE + 8)),

                                                   finishAtomicPtr_(reinterpret_cast<std::atomic<int64_t> *>(alignedPtr + 3 * CACHE_LINE_SIZE)),
                                                   capacity_(*reinterpret_cast<int64_t *>(alignedPtr + CACHE_LINE_SIZE)),
                                                   mask_(capacity_ - 1),
                                                   ipc_(ipc),
                                                   mmapLen_(len)
    {
    }

  public:
    SPSCQueueBase(long alignedPtr) : SPSCQueueBase(alignedPtr, false, 0)
    {
    }
    SPSCQueueBase(const char* fileName, int64_t len): SPSCQueueBase(mmap(fileName, len), true, len)
    {
    }

    ~SPSCQueueBase() {}

    static int64_t mmap(const char* fileName, int64_t len)
    {
        int fd = open(fileName, O_RDWR);
        assert(fd!=-1);
        //Execute mmap
        void* mmappedData = ::mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        assert(mmappedData != MAP_FAILED);
        ::close(fd);
        printf("MMap %s file to address %lld with length %lld.\n", fileName, reinterpret_cast<int64_t>(mmappedData), len);
        return reinterpret_cast<int64_t>(mmappedData);
    }

    void close() {
        if(ipc_) {
            int rc = munmap(reinterpret_cast<void*>(alignedRaw_), mmapLen_);
            assert(rc==0);
        }
    }

  private:
    static inline int64_t getLong(const int64_t *ptr)
    {
        return ptr[0];
    }

    static inline void putLong(int64_t *ptr, int64_t value)
    {
        ptr[0] = value;
    }

    static inline int64_t getLongVolatile(std::atomic<int64_t> *ptr)
    {
        return ptr->load();
    }

    static inline void putOrderedLong(std::atomic<int64_t> *ptr, int64_t value)
    {
        //corresponding to Java unsafe.putOrderedLong()
        //see https://github.com/unofficial-openjdk/openjdk/blob/4c7c4ac6e665335c6ddf47c703844c41b47dce08/src/jdk.unsupported/share/classes/sun/misc/Unsafe.java
        ptr->store(value, std::memory_order_release);
    }

  protected:
    int64_t getReadPlain()
    {
        return getLong(readPtr_);
    }

    int64_t getRead()
    {
        return getLongVolatile(readAtomicPtr_);
    }

    void setRead(int64_t value)
    {
        putOrderedLong(readAtomicPtr_, value);
    }

    int64_t getWritePlain()
    {
        return getLong(writePtr_);
    }

    int64_t getWrite()
    {
        return getLongVolatile(writeAtomicPtr_);
    }

    void setWrite(int64_t value)
    {
        putOrderedLong(writeAtomicPtr_, value);
    }

    int64_t getReadCache()
    {
        return getLong(readCachePtr_);
    }

    int64_t getWriteCache()
    {
        return getLong(writeCachePtr_);
    }

    void setReadCache(int64_t value)
    {
        putLong(readCachePtr_, value);
    }

    void setWriteCache(int64_t value)
    {
        putLong(writeCachePtr_, value);
    }

    void markFinished()
    {
        putOrderedLong(finishAtomicPtr_, -1);
    }

    bool isFinished()
    {
        int64_t v = getLongVolatile(finishAtomicPtr_);
        return v != 0;
    }

    inline int64_t nextWrap(int64_t v)
    {
        if ((v & mask_) == 0)
        {
            return v + capacity_;
        }
        return (v + mask_) & ~mask_;
    }

    inline int64_t prevWrap(int64_t v)
    {
        return v & ~mask_;
    }
};

class SPSCQueueInputStream : public SPSCQueueBase
{
  private:
    //the bytes offered by Next()
    int toMove_;

    inline int64_t updateReadPtr()
    {
        //as read address will only be written by this consumer,
        // thus, no need to enforce atomic for this variable
        int64_t currentRead = getReadPlain();
        if(toMove_ != 0) {
            currentRead += toMove_;
            setRead(currentRead);
            assert(toMove_ > 0);
        }

        toMove_ = 0;
        return currentRead;
    }
  public:
    SPSCQueueInputStream(int64_t alignedPtr) : SPSCQueueBase(alignedPtr), toMove_(0) {}
    SPSCQueueInputStream(const char* fileName, int64_t len): SPSCQueueBase(fileName, len), toMove_(0) {}

    // Obtains a chunk of data from the stream.
    //
    // Preconditions:
    // * "size" and "data" are not NULL.
    //
    // Postconditions:
    // * If the returned value is false, there is no more data to return or
    //   an error occurred.  All errors are permanent.
    // * Otherwise, "size" points to the actual number of bytes read and "data"
    //   points to a pointer to a buffer containing these bytes.
    // * Ownership of this buffer remains with the stream, and the buffer
    //   remains valid only until some other method of the stream is called
    //   or the stream is destroyed.
    // * It is legal for the returned buffer to have zero size, as long
    //   as repeatedly calling Next() eventually yields a buffer with non-zero
    //   size.
    virtual bool Next(const void **data, int *size)
    {
        //as read address will only be written by this consumer,
        // thus, no need to enforce atomic for this variable
        int64_t currentRead = updateReadPtr();
        int64_t writeCache = getWriteCache();
        //if read catches write cache, means there are no data to read
        while (currentRead >= writeCache)
        {
            //get write must be careful about memory order as it is written by another thread
            setWriteCache(getWrite());
            writeCache = getWriteCache();
            if (currentRead >= writeCache)
            {
                if (isFinished())
                {
                    //double check write ptr as there maybe an writer update between getWrite() and isFinished.
                    //we only ensure the memory ordering, so, if we see finish flag, then we must can see the final write flag
                    setWriteCache(getWrite());
                    writeCache = getWriteCache();
                    if (currentRead >= writeCache)
                    {
                        return false;
                    }
                    break;
                }
                std::this_thread::yield();
            }
        }
        //check whether there is a wrap in ring buffer read
        long nextReadWrap = nextWrap(currentRead);

        int avail = writeCache > nextReadWrap ? (int)(nextReadWrap - currentRead) : (int)(writeCache - currentRead);

        size[0] = avail;
        data[0] = reinterpret_cast<const void *>(arrayBase_ + (currentRead & mask_));
        //we can't setRead(currentRead+avail) here as the returned data needs to be protected from writing
        toMove_ = avail;
        //setRead(currentRead + avail);
        //printf("Read: array[%lld]=%d, avail=%d\n", currentRead, *(uint32_t*)(arrayBase_+currentRead), avail);
        return true;
    }

    // Backs up a number of bytes, so that the next call to Next() returns
    // data again that was already returned by the last call to Next().  This
    // is useful when writing procedures that are only supposed to read up
    // to a certain point in the input, then return.  If Next() returns a
    // buffer that goes beyond what you wanted to read, you can use BackUp()
    // to return to the point where you intended to finish.
    //
    // Preconditions:
    // * The last method called must have been Next().
    // * count must be less than or equal to the size of the last buffer
    //   returned by Next().
    //
    // Postconditions:
    // * The last "count" bytes of the last buffer returned by Next() will be
    //   pushed back into the stream.  Subsequent calls to Next() will return
    //   the same data again before producing new data.
    virtual void BackUp(int count)
    {
        toMove_ -= count;
        updateReadPtr();
    }

    // Skips a number of bytes.  Returns false if the end of the stream is
    // reached or some input error occurred.  In the end-of-stream case, the
    // stream is advanced to the end of the stream (so ByteCount() will return
    // the total size of the stream).
    virtual bool Skip(int count)
    {
        int remain = count;
        while (remain > 0)
        {
            const void *data;
            int size;
            if (!Next(&data, &size))
            {
                return false;
            }
            if (size > remain)
            {
                BackUp(size - remain);
                remain = 0;
            }
            else
            {
                remain -= size;
            }
        }
        return true;
    }

    /**
    Read a byte array to buf.
    @param buf  the buf to store result
    @param sizeToRead  number of bytes to read
    @return the actual bytes read
    */
    int readBytes(void *buf, size_t sizeToRead)
    {
        const void *data;
        int size;
        int remain = sizeToRead;
        char *p = reinterpret_cast<char *>(buf);

        while (Next(&data, &size))
        {
            const char *q = reinterpret_cast<const char *>(data);
            int s = remain > size ? size : remain;
            std::memcpy(p + sizeToRead - remain, q, s);
            if (size >= remain)
            {
                if (size > remain)
                {
                    BackUp(size - remain);
                }
                return sizeToRead;
            }
            remain -= s;
        }
        return sizeToRead - remain;
    }

    std::string readString()
    {
        const void *data;
        int size;
        std::string prev;

        while (Next(&data, &size))
        {
            const char *p = reinterpret_cast<const char *>(data);
            const char *q = p;
            while (q[0] != 0 && q < p + size)
            {
                q++;
            }
            if (q < p + size)
            {
                BackUp(p + size - q - 1);
                if (prev.empty())
                {
                    return p;
                }
                else
                {
                    prev += p;
                    return prev;
                }
            }
            //across multiple buffer
            prev += std::string(p, size);
        }
        return prev;
    }

    uint32_t readInt()
    {
        uint32_t result;
        readBytes(&result, sizeof(uint32_t));

        return result;
    }
};

class SPSCQueueOutputStream : public SPSCQueueBase
{
  private:
    //the bytes offered by Next()
    int toMove_;

    inline int64_t updateWritePtr() {
        int64_t currentWrite = getWritePlain();
        if(toMove_ != 0) {
            currentWrite += toMove_;
            setWrite(currentWrite);
            assert(toMove_ > 0);
        }
        toMove_ = 0;
        return currentWrite;
    }
  public:
    SPSCQueueOutputStream(int64_t alignedPtr) : SPSCQueueBase(alignedPtr), toMove_(0) {}
    SPSCQueueOutputStream(const char* fileName, int64_t len): SPSCQueueBase(fileName, len), toMove_(0) {}

    // Obtains a buffer into which data can be written.  Any data written
    // into this buffer will eventually (maybe instantly, maybe later on)
    // be written to the output.
    //
    // Preconditions:
    // * "size" and "data" are not NULL.
    //
    // Postconditions:
    // * If the returned value is false, an error occurred.  All errors are
    //   permanent.
    // * Otherwise, "size" points to the actual number of bytes in the buffer
    //   and "data" points to the buffer.
    // * Ownership of this buffer remains with the stream, and the buffer
    //   remains valid only until some other method of the stream is called
    //   or the stream is destroyed.
    // * Any data which the caller stores in this buffer will eventually be
    //   written to the output (unless BackUp() is called).
    // * It is legal for the returned buffer to have zero size, as long
    //   as repeatedly calling Next() eventually yields a buffer with non-zero
    //   size.
    virtual bool Next(void **data, int *size)
    {
        int capacity = capacity_;
        int64_t currentWrite = updateWritePtr();
        int64_t readWatermark = currentWrite - capacity;
        //if the ring queue is full as read leaves behind too much
        //wait until there is enough space to write
        while (getReadCache() <= readWatermark)
        {
            setReadCache(getRead());
            if (getReadCache() <= readWatermark)
            {
                std::this_thread::yield();
            }
        }

        int64_t prevWriteWrap = prevWrap(currentWrite);
        int64_t nextWriteWrap = nextWrap(currentWrite);

        int bytesToWrite;
        if (getReadCache() >= prevWriteWrap)
        {
            //read ptr and write ptr in the same window
            //this means the space from currentWrite to nextWriteWrap are all free to write
            bytesToWrite = (int)(nextWriteWrap - currentWrite);
        }
        else
        {
            bytesToWrite = capacity - (currentWrite - getReadCache());
        }
        size[0] = bytesToWrite;
        data[0] = reinterpret_cast<void *>(arrayBase_ + (currentWrite & mask_));
        toMove_ = bytesToWrite;
        //can't setWrite(currentWrite+bytesToWrite) here as the returned space shall be protected from reading
        //as the space may has not been written yet.
        //setWrite(currentWrite + bytesToWrite);

        return true;
    }

    // Backs up a number of bytes, so that the end of the last buffer returned
    // by Next() is not actually written.  This is needed when you finish
    // writing all the data you want to write, but the last buffer was bigger
    // than you needed.  You don't want to write a bunch of garbage after the
    // end of your data, so you use BackUp() to back up.
    //
    // Preconditions:
    // * The last method called must have been Next().
    // * count must be less than or equal to the size of the last buffer
    //   returned by Next().
    // * The caller must not have written anything to the last "count" bytes
    //   of that buffer.
    //
    // Postconditions:
    // * The last "count" bytes of the last buffer returned by Next() will be
    //   ignored.
    virtual void BackUp(int count)
    {
        toMove_ -= count;
        updateWritePtr();
    }

    void close()
    {
        updateWritePtr();
        markFinished();
    }

    void writeBytes(const char *val, size_t sizeToWrite)
    {
        void *data;
        int size;

        int total = sizeToWrite;
        int remain = total;
        const char *p = val;

        while (Next(&data, &size))
        {
            char *q = reinterpret_cast<char *>(data);
            int s = remain > size ? size : remain;
            std::memcpy(q, p + total - remain, s);
            if (size >= remain)
            {
                if (size > remain)
                {
                    BackUp(size - remain);
                }
                return;
            }
            remain -= s;
        }
    }

    void writeInt(uint32_t val)
    {
        const char *p = reinterpret_cast<const char *>(&val);
        writeBytes(p, sizeof(val));
    }

    void writeString(const char *val)
    {
        void *data;
        int size;
        const char *p = val;
        bool finishWrite = false;
        while (Next(&data, &size))
        {
            char *q = reinterpret_cast<char *>(data);
            int i = 0;
            for (i = 0; i < size; i++)
            {
                q[i] = p[i];
                if (p[i] == 0)
                {
                    finishWrite = true;
                    break;
                }
            }
            if (finishWrite)
            {
                BackUp(size - i - 1);
                return;
            }
            else
            {
                p += size;
            }
        }
    }
};

#endif