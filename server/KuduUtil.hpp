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
#pragma once

#include <crossbow/string.hpp>

#include <kudu/client/client.h>
#include <kudu/client/row_result.h>

using namespace kudu;
using namespace kudu::client;

using ScannerList = std::vector<std::unique_ptr<KuduScanner>>;
using Session = std::tr1::shared_ptr<kudu::client::KuduSession>;

void assertOk(Status status);

template<class T>
struct is_string {
    constexpr static bool value = false;
};

template<size_t S>
struct is_string<char[S]> {
    constexpr static bool value = true;
};

template<>
struct is_string<const char*> {
    constexpr static bool value = true;
};

template<>
struct is_string<std::string> {
    constexpr static bool value = true;
};

template<class T>
struct CreateValue {
    template<class U = T>
    typename std::enable_if<std::is_integral<U>::value, KuduValue*>::type
    create(U v) {
        return KuduValue::FromInt(v);
    }

    template<class U = T>
    typename std::enable_if<is_string<U>::value, KuduValue*>::type
    create(const U& v) {
        return KuduValue::CopyString(v);
    }
};


template<class... P>
struct PredicateAdder;

template<class Key, class Value, class CompOp, class... Tail>
struct PredicateAdder<Key, Value, CompOp, Tail...> {
    PredicateAdder<Tail...> tail;
    CreateValue<Value> creator;

    void operator() (KuduTable& table, KuduScanner& scanner, const Key& key,
            const Value& value, const CompOp& predicate, const Tail&... rest) {
        assertOk(scanner.AddConjunctPredicate(table.NewComparisonPredicate(key, predicate, creator.template create<Value>(value))));
        tail(table, scanner, rest...);
    }
};

template<>
struct PredicateAdder<> {
    void operator() (KuduTable& table, KuduScanner& scanner) {
    }
};

template<class... Args>
void addPredicates(KuduTable& table, KuduScanner& scanner, const Args&... args) {
    PredicateAdder<Args...> adder;
    adder(table, scanner, args...);
}

// opens a scan an puts it in the scanner list, use an empty projection-vector for getting everything
template<class... Args>
KuduScanner &openScan(KuduTable& table, ScannerList& scanners, const std::vector<std::string> &projectionColumns, const Args&... args) {
    scanners.emplace_back(new KuduScanner(&table));
    auto& scanner = *scanners.back();
    addPredicates(table, scanner, args...);
    if (projectionColumns.size() > 0) {
        scanner.SetProjectedColumnNames(projectionColumns);
    }
    assertOk(scanner.Open());
    return scanner;
}

// simple get request (retrieves one tuple)
template<class... Args>
KuduRowResult get(KuduTable& table, ScannerList& scanners, const Args&... args) {
    std::vector<std::string> emptyVec;
    auto& scanner = openScan(table, scanners, emptyVec, args...);
    assert(scanner.HasMoreRows());
    std::vector<KuduRowResult> rows;
    assertOk(scanner.NextBatch(&rows));
    return rows[0];
}

void set(KuduWriteOperation& upd, const Slice& slice, int16_t v);
void set(KuduWriteOperation& upd, const Slice& slice, int32_t v);
void set(KuduWriteOperation& upd, const Slice& slice, int64_t v);
void set(KuduWriteOperation& upd, const Slice& slice, double v);
void set(KuduWriteOperation& upd, const Slice& slice, std::nullptr_t);
void set(KuduWriteOperation& upd, const Slice& slice, const crossbow::string& str);
void set(KuduWriteOperation& upd, const Slice& slice, const Slice& str);
void getField(KuduRowResult &row, const std::string &columnName, int16_t &result);
void getField(KuduRowResult &row, const std::string &columnName, int32_t &result);
void getField(KuduRowResult &row, const std::string &columnName, int64_t &result);
void getField(KuduRowResult &row, const std::string &columnName, double &result);
void increaseAffectedRowsAndFlush(int32_t &affectedRows, kudu::client::KuduSession &session);

std::vector<const kudu::KuduPartialRow*> createRangePartitioning(int numItems, int partitions, kudu::client::KuduSchema& schema);
