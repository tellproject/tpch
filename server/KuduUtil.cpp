/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Markus Pilman <mpilman@inf.ethz.ch>
 *     Simon Loesing <sloesing@inf.ethz.ch>
 *     Thomas Etter <etterth@gmail.com>
 *     Kevin Bocksrocker <kevin.bocksrocker@gmail.com>
 *     Lucas Braun <braunl@inf.ethz.ch>
 */
#include "KuduUtil.hpp"

#include <vector>

#include <crossbow/logger.hpp>

void assertOk(Status status) {
    if (!status.ok()) {
        LOG_ERROR("ERROR from Kudu: %1%", status.message().ToString());
        throw std::runtime_error(status.message().ToString().c_str());
    }
}

void set(KuduWriteOperation& upd, const Slice& slice, int16_t v) {
    assertOk(upd.mutable_row()->SetInt16(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, int32_t v) {
    assertOk(upd.mutable_row()->SetInt32(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, int64_t v) {
    assertOk(upd.mutable_row()->SetInt64(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, double v) {
    assertOk(upd.mutable_row()->SetDouble(slice, v));
}

void set(KuduWriteOperation& upd, const Slice& slice, std::nullptr_t) {
    assertOk(upd.mutable_row()->SetNull(slice));
}

void set(KuduWriteOperation& upd, const Slice& slice, const crossbow::string& str) {
    assertOk(upd.mutable_row()->SetString(slice, std::string(str.c_str(), str.size())));
}

void set(KuduWriteOperation& upd, const Slice& slice, const Slice& str) {
    assertOk(upd.mutable_row()->SetString(slice, str));
}

void getField(KuduRowResult &row, const std::string &columnName, int16_t &result) {
    assertOk(row.GetInt16(columnName, &result));
}

void getField(KuduRowResult &row, const std::string &columnName, int32_t &result) {
    assertOk(row.GetInt32(columnName, &result));
}

void getField(KuduRowResult &row, const std::string &columnName, int64_t &result) {
    assertOk(row.GetInt64(columnName, &result));
}

void getField(KuduRowResult &row, const std::string &columnName, double &result) {
    assertOk(row.GetDouble(columnName, &result));
}

void increaseAffectedRowsAndFlush(int32_t &affectedRows, kudu::client::KuduSession &session) {
    affectedRows++;
    if (!(affectedRows % 1000))
        assertOk(session.Flush());
}

std::vector<const kudu::KuduPartialRow*> createRangePartitioning(int numItems, int partitions, kudu::client::KuduSchema& schema) {
    std::vector<const kudu::KuduPartialRow*> splits;
    int increment = numItems / partitions;
    for (int i = 1; i < partitions; ++i) {
        auto row = schema.NewRow();
        assertOk(row->SetInt32(0, i*increment));
        splits.emplace_back(row);
    }
    return splits;
}
