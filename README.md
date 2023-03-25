# OffHeap Lists, OffHeap Queues, and OffHeap Memory Mapped Files Fifo Queue

Purpose of this project is to use Off Heap memory;  
to achieve better low latency messaging between publisher/subscriber threads; without using synchronization, without locking, and without creating garbage !
 
This is a training and experimenting exercise for low latency programming.
  
There is a limitation of how much RAM the machine has; and the solution to that is to use Memory Mapped Files (see Chronicle Queue, Chronicle Index, Chronicle Map)

## OffHeapUnsafeList

List can be used as a messaging Topic; as it grows in size; and does not get emptied.
Subscribers can get the whole list content at any time; and can seek to first, last or at a given position.

Uses sun.misc.Unsafe to allocate memory.
Deallocation of memory must be called manually on Unsafe.freeMemory(address) after use. 
Otherwise memory leak will occur.

Maximum capacity to allocate is Long.MAX_VALUE (limitation of Unsafe class)

Use ListAppender to add elements to the list
Use ListTailer to read elements from the list 

Note: List does not support removal of items

## OffHeapDirectByteBufferList

List can be used as a messaging Topic; as it grows in size; and does not get emptied.
Subscribers can get the whole list content at any time; and can seek to first, last or at a given position.

Uses JDK DirectByteBuffer (safe) to allocate memory.
Deallocation of memory happens automatically when the GC runs.

Maximum capacity to allocate is Integer.MAX_VALUE (limitation of ByteBuffer class)

Use ListAppender to add elements to the list
Use ListTailer to read elements from the list 

Note: List does not support removal of items

## OffHeapUnsafeQueue

Queue can be used for distributing tasks to various threads.
It is a FIFO (first in first out).
It is Cyclic (aka Ring Buffer); when QueueAppender reaches the end of allocated memory; 
it starts writing at the beginning of the allocated memory again.
Therefore; work can be lost if no subscribers are taking out work

Uses sun.misc.Unsafe to allocate memory.
Deallocation of memory must be called manually on Unsafe.freeMemory(address) after use. 
Otherwise memory leak will occur.

Maximum capacity to allocate is Long.MAX_VALUE (limitation of Unsafe class)

Use QueueAppender to add elements to the queue
Use QueueTailer to read elements from the queue 

## OffHeapDirectByteBufferQueue

Queue can be used for distributing tasks to various threads.
It is a FIFO (first in first out).
It is Cyclic (aka Ring Buffer); when QueueAppender reaches the end of allocated memory; 
it starts writing at the beginning of the allocated memory again.
Therefore; work can be lost if no subscribers are taking out work

Uses JDK DirectByteBuffer (safe) to allocate memory.
Deallocation of memory happens automatically when the GC runs.

Maximum capacity to allocate is Integer.MAX_VALUE (limitation of ByteBuffer class)

Use QueueAppender to add elements to the queue
Use QueueTailer to read elements from the queue 

## OffHeapMMFQueue - Off Heap Memory Mapped File FIFO Queue

Queue can be used for distributing tasks to various threads.
It is a FIFO (first in first out).

Uses Memory Mapped Files

Supports multi threads, and multi-processors

Maximum capacity to allocate is Long.MAX_VALUE


## Test changes 20230324
## Test changes 20230325 01
## Test changes 20230325 02

## Test changes 20230325 JIRA-0002
## Test changes 20230325 feature/JIRA-0003
## Test changes 20230325 feature/JIRA-0004
