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
#include "Transactions.hpp"

using namespace tell::db;

namespace tpch {

RF1Out Transactions::rf1(tell::db::Transaction &tx, const RF1In &in)
{
    RF1Out result;
    try {
        auto oFuture = tx.openTable("orders");
        auto lFuture = tx.openTable("lineitem");

        auto lTable = lFuture.get();
        auto oTable = oFuture.get();

        tell::db::Counter lineitemCounter(tx.getCounter("lineitem_idx_counter"));

        for (auto &order: in.orders) {
            tx.insert(oTable, tell::db::key_t{uint64_t(order.orderkey)},
                    {{
                    {"o_orderkey", order.orderkey},
                    {"o_custkey", order.custkey},
                    {"o_orderstatus", order.orderstatus},
                    {"o_totalprice", order.totalprice},
                    {"o_orderdate", order.orderdate},
                    {"o_orderpriority", order.orderpriority},
                    {"o_clerk", order.clerk},
                    {"o_shippriority", order.shippriority},
                    {"o_comment", order.comment}
                    }});
            result.affectedRows++;
            for (auto &line: order.lineitems) {
                tx.insert(lTable, tell::db::key_t{lineitemCounter.next()},
                    {{
                    {"l_orderkey", line.orderkey},
                    {"l_linenumber", line.linenumber},
                    {"l_partkey", line.partkey},
                    {"l_suppkey", line.suppkey},
                    {"l_quantity", line.quantity},
                    {"l_extendedprice", line.extendedprice},
                    {"l_discount", line.discount},
                    {"l_tax", line.tax},
                    {"l_returnflag", line.returnflag},
                    {"l_linestatus", line.linestatus},
                    {"l_shipdate", line.shipdate},
                    {"l_commitdate", line.commitdate},
                    {"l_receiptdate", line.receiptdate},
                    {"l_shipinstruct", line.shipinstruct},
                    {"l_shipmode", line.shipmode},
                    {"l_comment", line.comment},
                    }});
                result.affectedRows++;
            }
        }
        tx.commit();
    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;

}

RF2Out Transactions::rf2(tell::db::Transaction &tx, const RF2In &in)
{
    RF2Out result;
    try {
        auto oFuture = tx.openTable("orders");
        auto lFuture = tx.openTable("lineitem");

        auto lTable = lFuture.get();
        auto oTable = oFuture.get();

        std::vector<Future<Tuple>> oTupleFutures;
        oTupleFutures.reserve(in.orderIds.size());
        std::vector<tell::db::key_t> orderKeys;
        orderKeys.reserve(in.orderIds.size());

        // get futures in revers order
        for (auto orderIdIter = in.orderIds.rbegin(); orderIdIter < in.orderIds.rend(); ++orderIdIter) {
            orderKeys.emplace_back(tell::db::key_t{uint64_t(*orderIdIter)});
            oTupleFutures.emplace_back(tx.get(oTable, orderKeys.back()));
        }

        auto orderKeyIter = orderKeys.begin();
        // get the actual values in reverse reverse = actual order
        for (auto iter = oTupleFutures.rbegin();
                    iter < oTupleFutures.rend(); ++iter, ++orderKeyIter) {
            auto &orderKey = *orderKeyIter;
            // remove order
            tx.remove(oTable, orderKey, iter->get());
            result.affectedRows++;
            // collect tuple futures for lineitems that might get deleted
            std::vector<Future<Tuple>> lTupleFutures;
            auto lineIter = tx.lower_bound(lTable, "l_orderkey_idx", {Field(int32_t(orderKey))});
            uint counter = 0;
            while (!lineIter.done() && counter++ < 7) {
                lTupleFutures.emplace_back(tx.get(lTable, lineIter.value()));
                lineIter.next();
            }
            // delete lineitems if necessary
            for (auto lFutureIter = lTupleFutures.rbegin(); lFutureIter < lTupleFutures.rend(); ++lFutureIter) {
                auto & lineitem = lFutureIter->get();
                if (lineitem["l_orderkey"] == int32_t(orderKey)) {
                    tx.remove(lTable, orderKey, lineitem);
                    result.affectedRows++;
                }
            }
        }

    } catch (std::exception& ex) {
        result.success = false;
        result.error = ex.what();
    }
    return result;
}

} // namespace tpch
