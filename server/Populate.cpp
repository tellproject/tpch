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

void Populator::populatePart(Transaction &transaction, double scalingFactor)
{
//    part_t part;
//    mk_part(1, &part);
}

void Populator::populateAll(Transaction &transaction, double scalingFactor)
{
    populateRegions(transaction);
    populateNations(transaction);
}

} // namespace tpch
