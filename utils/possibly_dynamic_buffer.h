/* possibly_dynamic_buffer.h                                                          -*- C++ -*-
   Mich, 24 August 2016

   This file is part of MLDB. Copyright 2016 mldb.ai inc. All rights reserved.
*/

#pragma once

namespace MLDB {

// Made to init and hold a non POD array of dynamic size in a memory safe
// way.
template<typename T, size_t MAX_STACK_ENTRIES=4096/sizeof(T)>
struct PossiblyDynamicBuffer {
    PossiblyDynamicBuffer(size_t sz)
    : size_(sz)
    {
        if (onStack()) {
            std::uninitialized_fill(stackEntries, stackEntries + size_, T());
        } 
        else {
		heapEntries = new T[size_];
	    }
	}

	~PossiblyDynamicBuffer()
	{
	    if (onStack()) {
		for (size_t i = 0; i < size_;  ++i) {
		    stackEntries[i].~T();
		}
	    }
	    else {
		delete[] heapEntries;
	    }
	}

	size_t size_;

	bool onStack() const { return size_ <= MAX_STACK_ENTRIES; }

	T * data() { return onStack() ? stackEntries: heapEntries; }
	size_t size() const { return size_; }

	union {
	    T stackEntries[MAX_STACK_ENTRIES];
	    T * heapEntries;
	};

	private:
	    PossiblyDynamicBuffer(const PossiblyDynamicBuffer & other);
	    PossiblyDynamicBuffer & operator=(PossiblyDynamicBuffer & other);
    };

} // MLDB
