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

#include "dbgen/dsstypes.h"

using namespace tell::db;

namespace tpch {

void Populator::populateRegions(Transaction &transaction)
{
    auto tIdFuture   = transaction.openTable("region");
    auto table       = tIdFuture.get();
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
    auto tIdFuture   = transaction.openTable("nation");
    auto table       = tIdFuture.get();
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
        tell::db::key_t partKey = tell::db::key_t{static_cast<uint64_t>(partIndex)};
        transaction.insert(partTable, partKey, {{
            {"p_partkey", static_cast<int32_t>(part.partkey)},
            {"p_name", crossbow::string(part.name)},
            {"p_mfgr", crossbow::string(part.mfgr)},
            {"p_brand", crossbow::string(part.brand)},
            {"p_type", crossbow::string(part.type)},
            {"p_size", static_cast<int32_t>(part.size)},
            {"p_container", crossbow::string(part.container)},
            {"p_retailprice", static_cast<int64_t>(part.retailprice)},
            {"p_comment", crossbow::string(part.comment)}
        }});
        for (long snum = 0; snum < SUPP_PER_PART; ++snum) {
            // [TODO:] continue here
        }
    }
}

void Populator::setScalingFactor(float scalingFactor)
{
    if (!mInitialized) {
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

void Populator::populateStaticTables(Transaction &transaction)
{
    populateRegions(transaction);
    populateNations(transaction);
}

void Populator::populatePortion(Transaction &transaction, uint portionID, float scalingFactor)
{
    setScalingFactor(scalingFactor);
    populatePartAndPartSupp(transaction, portionID);
}

} // namespace tpch
