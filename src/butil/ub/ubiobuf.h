// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BUTIL_UBIOBUF_H
#define BUTIL_UBIOBUF_H

#ifdef BRPC_WITH_URMA

#include "butil/iobuf.h"

namespace butil {

class UBIOBuf : public IOBuf {
public:
    static const size_t DEFAULT_BLOCK_SIZE = IOBuf::DEFAULT_BLOCK_SIZE;
    typedef IOBuf::Area Area;
    static const Area INVALID_AREA = IOBuf::INVALID_AREA;

    UBIOBuf() {}
    UBIOBuf(const UBIOBuf& rhs) : IOBuf(rhs) {}
    UBIOBuf(const Movable& rhs);
    ~UBIOBuf() override { clear(); }
    void operator=(const UBIOBuf& rhs) { operator=(static_cast<const IOBuf&>(rhs)); }
    void operator=(const IOBuf& rhs) override;
    void operator=(const Movable& rhs) override;
    void operator=(const char* s) override;
    void operator=(const std::string& s) override;
    void append(const IOBuf& other) override;
    void append(const Movable& other) override;
    int push_back(char c) override;
    int append(void const* data, size_t count) override;
    int append_to_tiny_pool_with_fallback(void const* data, size_t count, size_t block_size);
    int appendv(const const_iovec vec[], size_t n) override;
    int appendv(const iovec* vec, size_t n) override
    { return appendv((const const_iovec*)vec, n); }
    int append(char const* s) override;
    int append(const std::string& s) override;
    int append_user_data(void* data, size_t size, std::function<void(void*)> deleter) override {
        return append_user_data_with_meta(data, size, std::move(deleter), 0);
    }
    int append_user_data_with_meta(void* data, size_t size,
                                   std::function<void(void*)> deleter,
                                   uint64_t meta) override;
    int resize(size_t n) override { return resize(n, '\0'); }
    int resize(size_t n, char c) override;
    Area reserve(size_t n) override;
    bool use_ub() const override { return true; }
    int normalize();
    static int normalize(IOBuf* buf);
    static bool has_ub_block(const IOBuf* buf);
    static size_t get_block_size();

private:
    static int append_to_tiny_pool(UBIOBuf* out, const void* data, size_t len);
    static int append_to_registered_ub_pool(UBIOBuf* out, const void* data, size_t len, bool use_tiny_pool);
    static int normalize_to_tiny_pool(IOBuf* buf);
};

class UBIOBufAsZeroCopyOutputStream
    : public google::protobuf::io::ZeroCopyOutputStream {
public:
    explicit UBIOBufAsZeroCopyOutputStream(UBIOBuf*);
    UBIOBufAsZeroCopyOutputStream(UBIOBuf*, uint32_t block_size);
    ~UBIOBufAsZeroCopyOutputStream();

    bool Next(void** data, int* size) override;
    void BackUp(int count) override;
    int64_t ByteCount() const override;

private:
    void _release_block();

    UBIOBuf* _buf;
    uint32_t _block_size;
    IOBuf::Block *_cur_block;
    int64_t _byte_count;
};

// Wrap UBIOBuf into output of snappy compression.
class UBIOBufAsSnappySink : public butil::snappy::Sink {
public:
    explicit UBIOBufAsSnappySink(butil::UBIOBuf& buf);
    virtual ~UBIOBufAsSnappySink() {}

    // Append "bytes[0,n-1]" to this.
    void Append(const char* bytes, size_t n) override;

    // Returns a writable buffer of the specified length for appending.
    char* GetAppendBuffer(size_t length, char* scratch) override;

private:
    char* _cur_buf;
    int _cur_len;
    butil::UBIOBuf* _buf;
    butil::UBIOBufAsZeroCopyOutputStream _buf_stream;
};

namespace ubiobuf {

size_t block_count();
size_t block_memory();
size_t num_hit_ub_threshold();
IOBuf::Block* get_ub_block_head();
int get_ub_block_count();
IOBuf::Block* get_tiny_pool_block_head();
int get_tiny_pool_block_count();
void remove_tls_ub_block_chain();
void remove_tls_tiny_pool_block_chain();

}  // namespace ubiobuf

}  // namespace butil

#endif
#endif  // BUTIL_UBIOBUF_H
