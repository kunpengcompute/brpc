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

#ifndef BRPC_URMA_CONTROL_H
#define BRPC_URMA_CONTROL_H
#include <array>
#include <cstdint>
#include <cstring>
#include "butil/iobuf.h"
namespace brpc {
namespace urma {
// READ-only subset of the original binary control format. Reserved trace and
// other-operation fields remain zero; no protobuf or trace dependency.
struct UrmaControlItem {
    uint32_t seq_value{0}, length_value{0}, mask{0}, token_id_value{0};
    uint64_t source_id_value{0}, addr_value{0};
    uint32_t seq() const { return seq_value; }
    uint32_t length() const { return length_value; }
    void set_seq(uint32_t v) { seq_value = v; }
    void set_length(uint32_t v) { length_value = v; }
    uint64_t source_id() const { return source_id_value; }
    uint64_t addr() const { return addr_value; }
    uint32_t token_id() const { return token_id_value; }
    bool has_source_id() const { return mask & 1; }
    bool has_addr() const { return mask & 4; }
    bool has_token_id() const { return mask & 8; }
    void set_source_id(uint64_t v) { source_id_value = v; mask |= 1; }
    void set_addr(uint64_t v) { addr_value = v; mask |= 4; }
    void set_token_id(uint32_t v) { token_id_value = v; mask |= 8; }
};
class UrmaControl {
public:
    static constexpr size_t kHeaderSize = 48;
    static constexpr size_t kItemSize = 40;
    static constexpr size_t kMaxItems = 99;
    struct Items {
        const UrmaControlItem* first;
        size_t count;
        const UrmaControlItem* begin() const { return first; }
        const UrmaControlItem* end() const { return first + count; }
    };
    size_t items_size() const { return _count; }
    UrmaControlItem* mutable_items_begin() { return _items.data(); }
    Items items() const { return {_items.data(), _count}; }
    UrmaControlItem* add_items() { return _count < kMaxItems ? &_items[_count++] : nullptr; }
    static void Put(char* p, uint64_t v, size_t n) {
        for (size_t i = 0; i < n; ++i) { p[n - 1 - i] = static_cast<char>(v & 255); v >>= 8; }
    }
    static uint64_t Get(const char* p, size_t n) {
        uint64_t v = 0;
        for (size_t i = 0; i < n; ++i) v = (v << 8) | static_cast<unsigned char>(p[i]);
        return v;
    }
    bool Encode(butil::IOBuf* out) const {
        if (!_count || _count > kMaxItems) return false;
        std::array<char, kHeaderSize + kMaxItems * kItemSize> bytes{};
        Put(bytes.data(), 0x55524331, 4); Put(bytes.data() + 4, 1, 2);
        Put(bytes.data() + 8, _count, 2);
        for (size_t i = 0; i < _count; ++i) {
            const auto& item = _items[i];
            if (item.seq() > UINT16_MAX || !item.length() || (item.mask & ~13u)) return false;
            char* p = bytes.data() + kHeaderSize + i * kItemSize;
            Put(p, item.seq(), 2); Put(p + 2, item.mask, 2);
            Put(p + 4, item.length(), 4); Put(p + 8, item.source_id(), 8);
            Put(p + 24, item.addr(), 8); Put(p + 32, item.token_id(), 4);
        }
        out->clear(); out->append(bytes.data(), kHeaderSize + _count * kItemSize);
        return true;
    }
    bool Decode(const butil::IOBuf& input) {
        if (input.size() < kHeaderSize || input.size() > 4032) return false;
        char h[kHeaderSize]; input.copy_to(h, sizeof(h));
        const size_t count = Get(h + 8, 2);
        if (Get(h, 4) != 0x55524331 || Get(h + 4, 2) != 1 ||
            !count || count > kMaxItems || input.size() != kHeaderSize + count * kItemSize ||
            Get(h + 6, 2) || Get(h + 10, 2) || Get(h + 12, 4) ||
            Get(h + 16, 8) || Get(h + 24, 8) || Get(h + 32, 8) || Get(h + 40, 8)) return false;
        *this = UrmaControl();
        butil::IOBuf remaining = input; remaining.pop_front(kHeaderSize);
        for (size_t i = 0; i < count; ++i) {
            char p[kItemSize]; remaining.cutn(p, sizeof(p));
            auto* item = add_items();
            item->seq_value = Get(p, 2); item->mask = Get(p + 2, 2);
            item->length_value = Get(p + 4, 4); item->source_id_value = Get(p + 8, 8);
            item->addr_value = Get(p + 24, 8); item->token_id_value = Get(p + 32, 4);
            if (!item->length() || (item->mask & ~13u) || Get(p + 16, 8) || Get(p + 36, 4) ||
                (!item->has_source_id() && item->source_id()) ||
                (!item->has_addr() && item->addr()) || (!item->has_token_id() && item->token_id())) return false;
        }
        return true;
    }
private:
    std::array<UrmaControlItem, kMaxItems> _items{};
    size_t _count{0};
};
}  // namespace urma
}  // namespace brpc
#endif
