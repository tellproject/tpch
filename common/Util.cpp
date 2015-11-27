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
#include "Util.hpp"

#include <iostream>
#include <sstream>
#include <chrono>
#include "boost/date_time/posix_time/posix_time.hpp"
namespace tpch {

// splitting strings
std::vector<std::string> split(const std::string& str, const char delim) {
    std::stringstream ss(str);
    std::string item;
    std::vector<std::string> result;
    while (std::getline(ss, item, delim)) {
        if (item.empty()) continue;
        result.push_back(std::move(item));
    }
    return result;
}

uint64_t convertSqlDateToLong(const std::string& dateString)
{
    using namespace boost::posix_time;

    ptime epoch = time_from_string("1970-01-01 00:00:00.000");
    ptime other = time_from_string(dateString);
    time_duration const diff = other - epoch;
    return diff.total_seconds();
}

int64_t now() {
    auto now = std::chrono::system_clock::now();
    return now.time_since_epoch().count();
}

}
