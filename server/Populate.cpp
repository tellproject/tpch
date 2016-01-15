/*
 * (C) Copyright 2015 ETH Zurich Systems Group (http://www.systems.ethz.ch/) and
 * others.
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
#include "Populate.hpp"
#include <telldb/Transaction.hpp>
#include <chrono>
#include <algorithm>

#include "dbgen/driver.h"

using namespace tell::db;

namespace tpch {

void Populator::populateRegions(Transaction &transaction)
{
    auto tFuture    = transaction.openTable("region");
    auto table      = tFuture.get();
    std::string line;
    std::ifstream infile("ch-tables/region.tbl");
    while (std::getline(infile, line)) {
        auto items = tpch::split(line, '|');
        if (items.size() != 3) {
            LOG_ERROR("region file must contain of 3-tuples!");
            return;
        }
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        tell::db::key_t key = tell::db::key_t{static_cast<uint64_t>(intKey)};
        transaction.insert(table, key, {{
            {"r_regionkey", intKey},
            {"r_name", crossbow::string(items[1])},
            {"r_comment", crossbow::string(items[2])}
        }});
    }
}

void Populator::populateNations(Transaction &transaction)
{
    auto tFuture   = transaction.openTable("nation");
    auto table     = tFuture.get();
    std::string line;
    std::ifstream infile("ch-tables/nation.tbl");
    while (std::getline(infile, line)) {
        auto items = tpch::split(line, '|');
        if (items.size() != 4) {
            LOG_ERROR("nation file must contain of 4-tuples!");
            return;
        }
        int16_t intKey = static_cast<int16_t>(std::stoi(items[0]));
        tell::db::key_t key = tell::db::key_t{static_cast<uint64_t>(intKey)};
        transaction.insert(table, key, {{
            {"n_nationkey", intKey},
            {"n_name", crossbow::string(items[1])},
            {"n_regionkey", static_cast<int16_t>(std::stoi(items[2]))},
            {"n_comment", crossbow::string(items[3])}
        }});
    }
}

void Populator::setScalingFactor(float scalingFactor)
{
    if (!mInitialized) {

        // copy-pasted from original driver.c, l. 672
//        force = 0;
//        set_seeds = 0;
//        scale = 1;
//        updates = 0;
//        step = -1;
//        tdefs[ORDER].base *=
//            ORDERS_PER_CUST;			/* have to do this after init */
//        tdefs[LINE].base *=
//            ORDERS_PER_CUST;			/* have to do this after init */
//        tdefs[ORDER_LINE].base *=
//            ORDERS_PER_CUST;			/* have to do this after init */
//        children = 1;
//        d_path = NULL;

        // copy-pasted from original driver.c, l. 716
        load_dists();
#ifdef RNG_TEST
        for (int i=0; i <= MAX_STREAM; i++)
            Seed[i].nCalls = 0;
 #endif
        /* have to do this after init */
        tdefs[NATION].base = nations.count;
        tdefs[REGION].base = regions.count;

        // copy-pasted from driver.c, l. 488
        if (scalingFactor < MIN_SCALE)
        {
            int i;
            int int_scale;

            scale = 1;
            int_scale = (int)(1000 * scalingFactor);
            for (i = PART; i < REGION; i++)
            {
                tdefs[i].base = (DSS_HUGE)(int_scale * tdefs[i].base)/1000;
                if (tdefs[i].base < 1)
                    tdefs[i].base = 1;
            }
        }
        else
            scale = (long) scalingFactor;
        if (scale > MAX_SCALE)
        {
            fprintf (stderr, "%s %5.0f %s\n\t%s\n\n",
                "NOTE: Data generation for scale factors >",
                MAX_SCALE,
                "GB is still in development,",
                "and is not yet supported.\n");
            fprintf (stderr,
                "Your resulting data set MAY NOT BE COMPLIANT!\n");
        }
        mInitialized = true;
    }
}

void Populator::populatePartAndPartSupp(Transaction &transaction, uint portionID)
{
    auto tPartFuture        = transaction.openTable("part");
    auto tPartSuppFuture    = transaction.openTable("partsupp");
    auto partSuppTable      = tPartSuppFuture.get();
    auto partTable          = tPartFuture.get();

    DSS_HUGE partIndex = (portionID-1) * 200 + 1;
    auto partEndIndex = partIndex + 200;

    part_t part;
    for (; partIndex < partEndIndex; ++partIndex) {
        mk_part(partIndex, &part);
        uint64_t keyBase = uint64_t(partIndex);
        tell::db::key_t partKey = tell::db::key_t{keyBase};
        transaction.insert(partTable, partKey, {{
            {"p_partkey", static_cast<int32_t>(part.partkey)},
            {"p_name", crossbow::string(part.name)},
            {"p_mfgr", crossbow::string(part.mfgr)},
            {"p_brand", crossbow::string(part.brand)},
            {"p_type", crossbow::string(part.type)},
            {"p_size", static_cast<int32_t>(part.size)},
            {"p_container", crossbow::string(part.container)},
            {"p_retailprice", double(part.retailprice)/100},
            {"p_comment", crossbow::string(part.comment)}
        }});
        keyBase <<= 32;
        for (long snum = 0; snum < SUPP_PER_PART; ++snum) {
            auto &partSupp = part.s[snum];
            tell::db::key_t partSuppKey = tell::db::key_t{keyBase | uint64_t(partSupp.suppkey)};
            transaction.insert(partSuppTable, partSuppKey, {{
                {"ps_partkey", static_cast<int32_t>(partSupp.partkey)},
                {"ps_suppkey", static_cast<int32_t>(partSupp.suppkey)},
                {"ps_availqty", static_cast<int32_t>(partSupp.qty)},
                {"ps_supplycost", double(partSupp.scost)/100},
                {"ps_comment", crossbow::string(partSupp.comment)}
            }});
        }
    }
}

void Populator::populateSupplier(Transaction &transaction, uint portionID)
{
    auto tFuture    = transaction.openTable("supplier");
    auto table      = tFuture.get();

    DSS_HUGE suppIndex = (portionID-1) * 10 + 1;
    auto suppEndIndex = suppIndex + 10;

    supplier_t supp;
    for (; suppIndex < suppEndIndex; ++suppIndex) {
        mk_supp(suppIndex, &supp);
        tell::db::key_t key = tell::db::key_t{uint64_t(suppIndex)};
        transaction.insert(table, key, {{
            {"s_suppkey", static_cast<int32_t>(supp.suppkey)},
            {"s_name", crossbow::string(supp.name)},
            {"s_address", crossbow::string(supp.address)},
            {"s_nationkey", static_cast<int16_t>(supp.nation_code)},
            {"s_phone", crossbow::string(supp.phone)},
            {"s_acctbal", double(supp.acctbal)/100},
            {"s_comment", crossbow::string(supp.comment)}
        }});
    }
}

void Populator::populateCustomer(Transaction &transaction, uint portionID)
{
    auto tFuture    = transaction.openTable("customer");
    auto table      = tFuture.get();

    DSS_HUGE custIndex = (portionID-1) * 150 + 1;
    auto custEndIndex = custIndex + 150;

    customer_t cust;
    for (; custIndex < custEndIndex; ++custIndex) {
        mk_cust(custIndex, &cust);
        tell::db::key_t key = tell::db::key_t{uint64_t(custIndex)};
        transaction.insert(table, key, {{
            {"c_custkey", static_cast<int32_t>(cust.custkey)},
            {"c_name", crossbow::string(cust.name)},
            {"c_address", crossbow::string(cust.address)},
            {"c_nationkey", static_cast<int16_t>(cust.nation_code)},
            {"c_phone", crossbow::string(cust.phone)},
            {"c_acctbal", double(cust.acctbal)/100},
            {"c_mktsegment", crossbow::string(cust.mktsegment)},
            {"c_comment", crossbow::string(cust.comment)}
        }});
    }
}

void Populator::populateOrdersAndLines(Transaction &transaction, uint portionID)
{
    auto tOrdersFuture      = transaction.openTable("orders");
    auto tLineItemFuture    = transaction.openTable("lineitem");
    auto lineItemTable      = tLineItemFuture.get();
    auto ordersTable        = tOrdersFuture.get();

    DSS_HUGE orderIndex = (portionID-1) * 1500 + 1;
    auto orderEndIndex = orderIndex + 1500;

    order_t ord;
    for (; orderIndex < orderEndIndex; ++orderIndex) {
        mk_order(orderIndex, &ord, 0);  // no updates
        uint64_t keyBase = uint64_t(orderIndex);
        tell::db::key_t orderKey = tell::db::key_t{keyBase};
        transaction.insert(ordersTable, orderKey, {{
            {"o_orderkey", static_cast<int32_t>(ord.okey)},
            {"o_custkey", static_cast<int32_t>(ord.custkey)},
            {"o_orderstatus", crossbow::string(&ord.orderstatus, 1)},
            {"o_totalprice", double(ord.totalprice)/100},
            {"o_orderdate", static_cast<int64_t>(convertSqlDateToMilliSecs(ord.odate))},
            {"o_orderpriority", crossbow::string(ord.opriority)},
            {"o_clerk", crossbow::string(ord.clerk)},
            {"o_shippriority", static_cast<int32_t>(ord.spriority)},
            {"o_comment", crossbow::string(ord.comment)}
        }});
        keyBase <<= 32;
        for (long line = 0; line < ord.lines; ++line) {
            auto &oLine = ord.l[line];
            tell::db::key_t oLineKey = tell::db::key_t{keyBase | uint64_t(oLine.lcnt)};
            transaction.insert(lineItemTable, oLineKey, {{
                {"l_orderkey", static_cast<int32_t>(oLine.okey)},
                {"l_partkey", static_cast<int32_t>(oLine.partkey)},
                {"l_suppkey", static_cast<int32_t>(oLine.suppkey)},
                {"l_linenumber", static_cast<int32_t>(oLine.lcnt)},
                {"l_quantity", double(oLine.quantity)/100},
                {"l_extendedprice", double(oLine.eprice)/100},
                {"l_discount", double(oLine.discount)/100},
                {"l_tax", double(oLine.tax)/100},
                {"l_returnflag", crossbow::string(&oLine.rflag[0], 1)},
                {"l_linestatus", crossbow::string(&oLine.lstatus[0], 1)},
                {"l_shipdate", static_cast<int64_t>(convertSqlDateToMilliSecs(oLine.sdate))},
                {"l_commitdate", static_cast<int64_t>(convertSqlDateToMilliSecs(oLine.cdate))},
                {"l_receiptdate", static_cast<int64_t>(convertSqlDateToMilliSecs(oLine.rdate))},
                {"l_shipinstruct", crossbow::string(oLine.shipinstruct)},
                {"l_shipmode", crossbow::string(oLine.shipmode)},
                {"l_comment", crossbow::string(oLine.comment)}
            }});
        }
    }
}

void Populator::populateStaticTables(Transaction &transaction)
{
    populateRegions(transaction);
    populateNations(transaction);
}

void Populator::populatePortion(Transaction &transaction, uint portionID, float scalingFactor)
{
    setScalingFactor(scalingFactor);
    populatePartAndPartSupp(transaction, portionID);
    populateSupplier(transaction, portionID);
    populateCustomer(transaction, portionID);
    populateOrdersAndLines(transaction, portionID);
}

} // namespace tpch
