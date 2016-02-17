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

uint64_t convertSqlDateToMilliSecs(const std::string& dateString)
{
    using namespace boost::posix_time;

    ptime epoch = time_from_string("1970-01-01 00:00:00.000");
    ptime other;
    if (dateString.find(" ") == std::string::npos)
        other = time_from_string(std::string(dateString + " 00:00:00.000"));
    else
        other = time_from_string(dateString);
    time_duration const diff = other - epoch;
    return diff.total_milliseconds();
}

bool file_readable(const std::string& fileName) {
    std::ifstream in(fileName.c_str());
    return in.good();
}

//template<class Fun>
//void getFiles(const std::string& baseDir, const std::string& fileName, const std::string &suffix, Fun fun, const bool includeParts) {
//    int part = 1;
//    auto fName = baseDir + "/" + fileName + "." + suffix;
//    if (file_readable(fName)) {
//        fun(fName);
//    }
//    while (includeParts) {
//        auto filename = fName + "." + std::to_string(part);
//        if (!file_readable(filename)) break;
//        fun(filename);
//        ++part;
//    }
//}

double getScalingFactor(const std::string& baseDir) {
    auto splits = split(baseDir, '/');
    return std::stod(splits[splits.size()-1]);
}

} // namespace tpch
