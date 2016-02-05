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

#include "TransactionsKudu.hpp"

#include <kudu/client/row_result.h>

#include <crossbow/logger.hpp>

void assertOk(kudu::Status status) {
    if (!status.ok()) {
        LOG_ERROR("ERROR from Kudu: %1%", status.message().ToString());
        throw std::runtime_error(status.message().ToString().c_str());
    }
}

using namespace kudu;
using namespace kudu::client;

using ScannerList = std::vector<std::unique_ptr<KuduScanner>>;

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

namespace tpch {

RF1Out TransactionsKudu::rf1(kudu::client::KuduSession &session, const RF1In &in)
{
    RF1Out result;
    LOG_DEBUG("Starting RF1 with " + std::to_string(in.orders.size()) + " orders.");

    try {
        std::tr1::shared_ptr<KuduTable> oTable;
        assertOk(session.client()->OpenTable("orders", &oTable));
        std::tr1::shared_ptr<KuduTable> lTable;
        assertOk(session.client()->OpenTable("lineitem", &lTable));

        for (auto &order: in.orders) {
            std::unique_ptr<KuduWriteOperation> oIns(oTable->NewInsert());
            set(*oIns, "o_orderkey", order.orderkey);
            set(*oIns, "o_custkey", order.custkey);
            set(*oIns, "o_orderstatus", order.orderstatus);
            set(*oIns, "o_totalprice", order.totalprice);
            set(*oIns, "o_orderdate", order.orderdate);
            set(*oIns, "o_orderpriority", order.orderpriority);
            set(*oIns, "o_clerk", order.clerk);
            set(*oIns, "o_shippriority", order.shippriority);
            set(*oIns, "o_comment", order.comment);
            assertOk(session.Apply(oIns.release()));
            increaseAffectedRowsAndFlush(result.affectedRows, session);

            for (auto &line: order.lineitems) {
                std::unique_ptr<KuduWriteOperation> lIns(lTable->NewInsert());
                set(*lIns, "l_orderkey", line.orderkey);
                set(*lIns, "l_partkey", line.partkey);
                set(*lIns, "l_suppkey", line.suppkey);
                set(*lIns, "l_linenumber", line.linenumber);
                set(*lIns, "l_quantity", line.quantity);
                set(*lIns, "l_extendedprice", line.extendedprice);
                set(*lIns, "l_discount", line.discount);
                set(*lIns, "l_tax", line.tax);
                set(*lIns, "l_returnflag", line.returnflag);
                set(*lIns, "l_linestatus", line.linestatus);
                set(*lIns, "l_shipdate", line.shipdate);
                set(*lIns, "l_commitdate", line.commitdate);
                set(*lIns, "l_receiptdate", line.receiptdate);
                set(*lIns, "l_shipinstruct", line.shipinstruct);
                set(*lIns, "l_shipmode", line.shipmode);
                set(*lIns, "l_comment", line.comment);
                assertOk(session.Apply(lIns.release()));
                increaseAffectedRowsAndFlush(result.affectedRows, session);
            }
        }
        assertOk(session.Flush());
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    LOG_DEBUG("Finishing RF1, " + std::to_string(result.affectedRows) + " rows affected.");
    return result;
}

RF2Out TransactionsKudu::rf2(kudu::client::KuduSession &session, const RF2In &in)
{
    RF2Out result;
    LOG_DEBUG("Starting RF2 with " + std::to_string(in.orderIds.size()) + " orders to delete.");

    try {
        std::tr1::shared_ptr<KuduTable> oTable;
        assertOk(session.client()->OpenTable("orders", &oTable));
        std::tr1::shared_ptr<KuduTable> lTable;
        assertOk(session.client()->OpenTable("lineitem", &lTable));

        std::vector<std::string> projection;
        projection.push_back("l_orderkey");
        projection.push_back("l_linenumber");

        for (int32_t orderId: in.orderIds) {
            std::unique_ptr<KuduWriteOperation> oDel(oTable->NewDelete());
            set(*oDel, "o_orderkey", orderId);
            assertOk(session.Apply(oDel.release()));
            increaseAffectedRowsAndFlush(result.affectedRows, session);

            ScannerList scanners;
            std::vector<KuduRowResult> resultBatch;
            KuduScanner &scanner = openScan(*lTable, scanners, projection, "l_orderkey", orderId, KuduPredicate::EQUAL);
            while (scanner.HasMoreRows()) {
                assertOk(scanner.NextBatch(&resultBatch));
                int32_t orderkey;
                int32_t linenumber;
                for (KuduRowResult row : resultBatch) {
                    getField(row, "l_orderkey", orderkey);
                    getField(row, "l_linenumber", linenumber);
                    std::unique_ptr<KuduWriteOperation> lDel(lTable->NewDelete());
                    set(*lDel, "l_orderkey", orderkey);
                    set(*lDel, "l_linenumber", linenumber);
                    assertOk(session.Apply(lDel.release()));
                    increaseAffectedRowsAndFlush(result.affectedRows, session);
                }
            }
        }
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }

    LOG_DEBUG("Finishing RF2, " + std::to_string(result.affectedRows) + " rows affected.");
    return result;
}

} // namespace tpch
