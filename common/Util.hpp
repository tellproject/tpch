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
#include <random>
#include <cstdint>
#include <string>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>

#include <crossbow/string.hpp>

#include <boost/date_time.hpp>
#include <boost/lexical_cast.hpp>

namespace tpch {

// splitting strings
std::vector<std::string> split(const std::string& str, const char delim);

// struct for handling dates
struct date {
    int64_t value;

    date() : value(0) {}

    date(const std::string& str) {
        using namespace boost::posix_time;
        ptime epoch(boost::gregorian::date(1970, 1, 1));
        auto time = time_from_string(str + " 00:00:00");
        value = (time - epoch).total_milliseconds();
    }

    operator int64_t() const {
        return value;
    }
};

// converts a SqlDate (with timestamp) to millisecos since 1.1.1970.
uint64_t convertSqlDateToMilliSecs(const std::string& dateString);

// is this a readable file?
bool file_readable(const std::string& fileName);

// get files from a base directory, with a certain filename (e.g. lineitem), and a certain suffix (e.g. tbl or tbl.u)
// if it should include parts, the function also searches for part files (e.g. lineitem.tbl.1)
// applies function fun to the readible files that match the description
template<class Fun>
void getFiles(const std::string& baseDir, const std::string& fileName,const std::string& suffix, Fun fun, const bool includeParts = true);

// extracts scalingFactor from baseDir (convention is that the name of this directory
// has to be equal to the scaling factor of the files it contains
double getScalingFactor(const std::string& baseDir);

// a bunch of functions that helps casting objects from crossbow strings and writing them to tuples
template<class Dest>
struct tpch_caster {
    Dest operator() (std::string&& str) const {
        return boost::lexical_cast<Dest>(str);
    }
};

template<>
struct tpch_caster<date> {
    date operator() (std::string&& str) const {
        return date(str);
    }
};

template<>
struct tpch_caster<std::string> {
    std::string operator() (std::string&& str) const {
        return std::move(str);
    }
};

template<>
struct tpch_caster<crossbow::string> {
    crossbow::string operator() (std::string&& str) const {
        return crossbow::string(str.begin(), str.end());
    }
};

template<class T, size_t P>
struct TupleWriter {
    TupleWriter<T, P - 1> next;
    void operator() (T& res, std::stringstream& ss) const {
        constexpr size_t total_size = std::tuple_size<T>::value;
        tpch_caster<typename std::tuple_element<total_size - P, T>::type> cast;
        std::string field;
        std::getline(ss, field, '|');
        std::get<total_size - P>(res) = cast(std::move(field));
        next(res, ss);
    }
};

template<class T>
struct TupleWriter<T, 0> {
    void operator() (T& res, std::stringstream& ss) const {
    }
};

// read tuples from an input stream and apply function fun to everyone of them
template<class Tuple, class Fun>
void getFields(std::istream& in, Fun fun) {
    std::string line;
    Tuple tuple;
    TupleWriter<Tuple, std::tuple_size<Tuple>::value> writer;
    while (std::getline(in, line)) {
        std::stringstream ss(line);
        writer(tuple, ss);
        fun(tuple);
    }
}

} // namespace tpch
