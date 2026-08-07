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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <deque>
#include <utility>
#include <vector>

#include <fuzzer/FuzzedDataProvider.h>

#include "brpc/memfd/shm_session.h"

namespace {

typedef std::pair<size_t, uint32_t> ModelElement;

struct DequeueState {
    std::vector<ModelElement> elements;
};

void CollectElement(void* arg, const brpc::ShmQueueElement* elem) {
    DequeueState* state = static_cast<DequeueState*>(arg);
    state->elements.push_back(ModelElement(elem->offset, elem->data_size));
}

class QueueHarness {
public:
    explicit QueueHarness(size_t capacity)
        : _capacity(capacity),
          _mem(sizeof(brpc::ShmQueueHeader) +
               capacity * sizeof(brpc::ShmQueueElement)),
          _active(false) {
        Init();
    }

    ~QueueHarness() {
        _queue.Reset();
    }

    void Init() {
        _queue.Reset();
        _model.clear();
        _active = _queue.Init(_mem.data(), _mem.size()) == 0;
        assert(_active);
        assert(_queue.Size() == 0);
    }

    void ResetOnly() {
        _queue.Reset();
        _model.clear();
        _active = false;
        assert(_queue.Size() == 0);
        assert(!_queue.IsFree());
    }

    void Enqueue(size_t offset, uint32_t data_size) {
        const int rc = _queue.Enqueue(offset, data_size);
        if (!_active) {
            assert(rc != 0);
            return;
        }
        if (_model.size() < _capacity) {
            assert(rc == 0);
            _model.push_back(ModelElement(offset, data_size));
        } else {
            assert(rc != 0);
        }
        assert(_queue.Size() == _model.size());
    }

    void Dequeue(size_t max_count) {
        DequeueState state;
        const int count =
            _queue.DequeueAndProcessBatch(CollectElement, &state, max_count);
        if (!_active || max_count == 0) {
            assert(count == 0);
            return;
        }
        const size_t expected_count = std::min(max_count, _model.size());
        assert(count == static_cast<int>(expected_count));
        assert(state.elements.size() == expected_count);
        for (size_t i = 0; i < expected_count; ++i) {
            assert(state.elements[i] == _model.front());
            _model.pop_front();
        }
        assert(_queue.Size() == _model.size());
    }

    void Fill(FuzzedDataProvider* provider) {
        const size_t attempts = _capacity + 2;
        for (size_t i = 0; i < attempts; ++i) {
            Enqueue(provider->ConsumeIntegral<size_t>(),
                    provider->ConsumeIntegral<uint32_t>());
        }
    }

    void ToggleReading(bool reading) {
        _queue.SetReading(reading);
        (void)_queue.IsNofify();
    }

private:
    size_t _capacity;
    std::vector<uint8_t> _mem;
    brpc::ShmQueue _queue;
    std::deque<ModelElement> _model;
    bool _active;
};

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0 || size > 4096) {
        return 0;
    }
    FuzzedDataProvider provider(data, size);
    QueueHarness harness(
        provider.ConsumeIntegralInRange<size_t>(1, 128));

    while (provider.remaining_bytes() > 0) {
        switch (provider.ConsumeIntegral<uint8_t>() % 7) {
        case 0:
            harness.Enqueue(provider.ConsumeIntegral<size_t>(),
                            provider.ConsumeIntegral<uint32_t>());
            break;
        case 1:
            harness.Dequeue(provider.ConsumeIntegralInRange<size_t>(0, 256));
            break;
        case 2:
            harness.Fill(&provider);
            break;
        case 3:
            harness.Dequeue(1);
            break;
        case 4:
            harness.ToggleReading(provider.ConsumeBool());
            break;
        case 5:
            harness.ResetOnly();
            break;
        default:
            harness.Init();
            break;
        }
    }
    return 0;
}
