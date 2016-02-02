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

#include <sstream>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <memory>
#include <thread>

#include <boost/lexical_cast.hpp>
#include <boost/date_time.hpp>

#include <telldb/TellDB.hpp>

#ifdef USE_KUDU
#include <kudu/client/client.h>
#endif

namespace tpch {

// public stuff

template<class ConnectionType, class FiberType>
void createSchemaAndPopulate(std::string &storage, std::string &commitManager, std::string baseDir);

using TellConnection = std::unique_ptr<tell::db::ClientManager<void>>;
extern template void createSchemaAndPopulate<TellConnection, tell::db::TransactionFiber<void>>(std::string &storage, std::string &commitManager, std::string baseDir);

#ifdef USE_KUDU
using KuduConnection = std::tr1::shared_ptr<kudu::client::KuduClient>;
extern template void createSchemaAndPopulate<KuduConnection, std::thread>(std::string &storage, std::string &commitManager, std::string baseDir);
#endif

// private stuff
enum class type {
    SMALLINT, INT, BIGINT, FLOAT, DOUBLE, TEXT
};

template<class T>
struct TableCreator;

template<class T>
void createPart(T& tx) {
    TableCreator<T> tc(tx);
    tc("p_partkey", type::INT);
    tc("p_name", type::TEXT);
    tc("p_mfgr", type::TEXT);
    tc("p_brand", type::TEXT);
    tc("p_type", type::TEXT);
    tc("p_size", type::INT);
    tc("p_container", type::TEXT);
    tc("p_retailprice", type::DOUBLE);
    tc("p_comment", type::TEXT);
    tc.setPrimaryKey({"p_partkey"});
    tc.create("part");
}

template<class T>
void createSupplier(T& tx) {
    TableCreator<T> tc(tx);
    tc("s_suppkey", type::INT);
    tc("s_name", type::TEXT);
    tc("s_address", type::TEXT);
    tc("s_nationkey", type::INT);
    tc("s_phone", type::TEXT);
    tc("s_acctbal", type::DOUBLE);
    tc("s_comment", type::TEXT);
    tc.setPrimaryKey({"s_suppkey"});
    tc.create("supplier");
}

template<class T>
void createPartsupp(T& tx) {
    TableCreator<T> tc(tx);
    tc("ps_partkey", type::INT);
    tc("ps_suppkey", type::INT);
    tc("ps_availqty", type::INT);
    tc("ps_supplycost", type::DOUBLE);
    tc("ps_comment", type::TEXT);
    tc.setPrimaryKey({"ps_partkey", "ps_suppkey"});
    tc.create("partsupp");
}

template<class T>
void createCustomer(T& tx) {
    TableCreator<T> tc(tx);
    tc("c_custkey", type::INT);
    tc("c_name", type::TEXT);
    tc("c_address", type::TEXT);
    tc("c_nationkey", type::INT);
    tc("c_phone", type::TEXT);
    tc("c_acctbal", type::DOUBLE);
    tc("c_mktsegment", type::TEXT);
    tc("c_comment", type::TEXT);
    tc.setPrimaryKey({"c_custkey"});
    tc.create("customer");
}

template<class T>
void createOrder(T& tx) {
    TableCreator<T> tc(tx);
    tc("o_orderkey", type::INT);
    tc("o_custkey", type::INT);
    tc("o_orderstatus", type::TEXT);
    tc("o_totalprice", type::DOUBLE);
    tc("o_orderdate", type::BIGINT);
    tc("o_orderpriority", type::TEXT);
    tc("o_clerk", type::TEXT);
    tc("o_shippriority", type::INT);
    tc("o_comment", type::TEXT);
    tc.setPrimaryKey({"o_orderkey"});
    tc.create("orders");
}

template<class T>
void createLineitem(T& tx) {
    TableCreator<T> tc(tx);
    tc("l_orderkey", type::INT);
    tc("l_linenumber", type::INT);
    tc("l_partkey", type::INT);
    tc("l_suppkey", type::INT);
    tc("l_quantity", type::DOUBLE);
    tc("l_extendedprice", type::DOUBLE);
    tc("l_discount", type::DOUBLE);
    tc("l_tax", type::DOUBLE);
    tc("l_returnflag", type::TEXT);
    tc("l_linestatus", type::TEXT);
    tc("l_shipdate", type::BIGINT);
    tc("l_commitdate", type::BIGINT);
    tc("l_receiptdate", type::BIGINT);
    tc("l_shipinstruct", type::TEXT);
    tc("l_shipmode", type::TEXT);
    tc("l_comment", type::TEXT);
    tc.setPrimaryKey({"l_orderkey", "l_linenumber"});
    tc.create("lineitem");
}

template<class T>
void createNation(T& tx) {
    TableCreator<T> tc(tx);
    tc("n_nationkey", type::INT);
    tc("n_name", type::TEXT);
    tc("n_regionkey", type::INT);
    tc("n_comment", type::TEXT);
    tc.setPrimaryKey({"n_nationkey"});
    tc.create("nation");
}

template<class T>
void createRegion(T& tx) {
    TableCreator<T> tc(tx);
    tc("r_regionkey", type::INT);
    tc("r_name", type::TEXT);
    tc("r_comment", type::TEXT);
    tc.setPrimaryKey({"r_regionkey"});
    tc.create("region");
}

template<class T>
void createTables(T& tx) {
    createPart(tx);
    createSupplier(tx);
    createPartsupp(tx);
    createCustomer(tx);
    createOrder(tx);
    createLineitem(tx);
    createNation(tx);
    createRegion(tx);
}

template<class T>
void createSchema(T& connection);

template<class T>
T getConnection(std::string &storage, std::string &commitMananger);

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

template<class T>
struct Populator;

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

template<class T>
struct string_type;

template<class T>
struct Populate {
    using string = typename string_type<T>::type;
    using P = Populator<T>;
    T& tx;

    Populate(T& tx)
        : tx(tx)
    {}

    void populatePart(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, string, string, string, string, int32_t, string, double, string>;
        P p(tx, "part");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("p_partkey",     std::get<0>(fields));
            p("p_name",        std::get<1>(fields));
            p("p_mfgr",        std::get<2>(fields));
            p("p_brand",       std::get<3>(fields));
            p("p_type",        std::get<4>(fields));
            p("p_size",        std::get<5>(fields));
            p("p_container",   std::get<6>(fields));
            p("p_retailprice", std::get<7>(fields));
            p("p_comment",     std::get<8>(fields));
            p.apply(startKey + count);
            if (++count % 1000u == 0u)
                p.flush();
        });
        p.flush();
    }

    void populateSupplier(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, string, string, int32_t, string, double, string>;
        P p(tx, "supplier");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("s_suppkey",   std::get<0>(fields));
            p("s_name",      std::get<1>(fields));
            p("s_address",   std::get<2>(fields));
            p("s_nationkey", std::get<3>(fields));
            p("s_phone",     std::get<4>(fields));
            p("s_acctbal",   std::get<5>(fields));
            p("s_comment",   std::get<6>(fields));
            p.apply(startKey + count);
            if (++count % 1000 == 0)
                p.flush();
        });
        p.flush();
    }

    void populatePartsupp(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, int32_t, int32_t, double, string>;
        P p(tx, "partsupp");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("ps_partkey",   std::get<0>(fields));
            p("ps_suppkey",   std::get<1>(fields));
            p("ps_availqty",  std::get<2>(fields));
            p("ps_supplycost",std::get<3>(fields));
            p("ps_comment",   std::get<4>(fields));
            p.apply(startKey + count);
            if (++count % 1000 == 0)
                p.flush();
        });
        p.flush();
    }

    void populateCustomer(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, string, string, int32_t, string, double, string, string>;
        P p(tx, "customer");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("c_custkey",    std::get<0>(fields));
            p("c_name",       std::get<1>(fields));
            p("c_address",    std::get<2>(fields));
            p("c_nationkey",  std::get<3>(fields));
            p("c_phone",      std::get<4>(fields));
            p("c_acctbal",    std::get<5>(fields));
            p("c_mktsegment", std::get<6>(fields));
            p("c_comment",    std::get<7>(fields));
            p.apply(startKey + count);
            if (++count % 1000 == 0)
                p.flush();
        });
        p.flush();
    }

    void populateOrder(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, int32_t, string, double, date, string, string, int32_t, string>;
        P p(tx, "orders");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("o_orderkey",     std::get<0>(fields));
            p("o_custkey",      std::get<1>(fields));
            p("o_orderstatus",  std::get<2>(fields));
            p("o_totalprice",   std::get<3>(fields));
            p("o_orderdate",    std::get<4>(fields));
            p("o_orderpriority",std::get<5>(fields));
            p("o_clerk",        std::get<6>(fields));
            p("o_shippriority", std::get<7>(fields));
            p("o_comment",      std::get<8>(fields));
            p.apply(startKey + count);
            if (++count % 1000 == 0)
                p.flush();
        });
        p.flush();
    }

    void populateLineitem(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, int32_t, int32_t, int32_t, double, double, double, double, string, string, date, date, date, string, string, string>;
        P p(tx, "lineitem");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("l_orderkey",      std::get<0>(fields));
            p("l_partkey",       std::get<1>(fields));
            p("l_suppkey",       std::get<2>(fields));
            p("l_linenumber",    std::get<3>(fields));
            p("l_quantity",      std::get<4>(fields));
            p("l_extendedprice", std::get<5>(fields));
            p("l_discount",      std::get<6>(fields));
            p("l_tax",           std::get<7>(fields));
            p("l_returnflag",    std::get<8>(fields));
            p("l_linestatus",    std::get<9>(fields));
            p("l_shipdate",      std::get<10>(fields));
            p("l_commitdate",    std::get<11>(fields));
            p("l_receiptdate",   std::get<12>(fields));
            p("l_shipinstruct",  std::get<13>(fields));
            p("l_shipmode",      std::get<14>(fields));
            p("l_comment",       std::get<15>(fields));
            p.apply(startKey + count);
            if (++count % 1000 == 0)
                p.flush();
        });
        p.flush();
    }

    void populateNation(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, string, int32_t, string>;
        P p(tx, "nation");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("n_nationkey", std::get<0>(fields));
            p("n_name",      std::get<1>(fields));
            p("n_regionkey", std::get<2>(fields));
            p("n_comment",   std::get<3>(fields));
            p.apply(startKey + count);
            if (++count % 1000 == 0)
                p.flush();
        });
        p.flush();
    }

    void populateRegion(std::istream& in, uint64_t startKey) {
        using t = std::tuple<int32_t, string, string>;
        P p(tx, "region");
        uint64_t count = 0;
        getFields<t>(in, [&count, &p, startKey] (const t& fields) {
            p("r_regionkey", std::get<0>(fields));
            p("r_name",      std::get<1>(fields));
            p("r_comment",   std::get<2>(fields));
            p.apply(startKey + count);
            if (++count % 1000 == 0)
                p.flush();
        });
        p.flush();
    }
};

template <class T>
void populateTable(std::string &tableName, const uint64_t &startKey, const std::shared_ptr<std::stringstream> &data, T &populate) {
    if (tableName == "part") {
        populate.populatePart(*data, startKey);
    } else if (tableName == "partsupp") {
        populate.populatePartsupp(*data, startKey);
    } else if (tableName == "supplier") {
        populate.populateSupplier(*data, startKey);
    } else if (tableName == "customer") {
        populate.populateCustomer(*data, startKey);
    } else if (tableName == "orders") {
        populate.populateOrder(*data, startKey);
    } else if (tableName == "lineitem") {
        populate.populateLineitem(*data, startKey);
    } else if (tableName == "nation") {
        populate.populateNation(*data, startKey);
    } else if (tableName == "region") {
        populate.populateRegion(*data, startKey);
    } else {
        std::cerr << "Table " << tableName << " does not exist" << std::endl;
        std::terminate();
    }
}

template<class ConnectionType, class FiberType>
void threaded_populate(ConnectionType &connection, std::queue<FiberType> &fibers,
        std::string &tableName, const uint64_t &startKey, const std::shared_ptr<std::stringstream> &data);

inline bool file_readable(const std::string& fileName) {
    std::ifstream in(fileName.c_str());
    return in.good();
}

template<class FiberType>
void join(FiberType &fiber);

template<class Fun>
void getFiles(const std::string& baseDir, const std::string& tableName, Fun fun) {
    int part = 1;
    auto fName = baseDir + "/" + tableName + ".tbl";
    if (file_readable(fName)) {
        fun(fName);
    }
    while (true) {
        auto filename = fName + "." + std::to_string(part);
        if (!file_readable(filename)) break;
        fun(filename);
        ++part;
    }
}

template<class ConnectionType, class FiberType>
void createSchemaAndPopulate(std::string &storage, std::string &commitManager, std::string baseDir) {
    ConnectionType connection = getConnection<ConnectionType>(storage, commitManager);
    createSchema(connection);

    for (std::string tableName : {"part", "partsupp", "supplier", "customer", "orders", "lineitem", "nation", "region"}) {
        getFiles(baseDir, tableName, [&connection, &tableName] (const std::string& fileName) {
        std::cout << "Reading " << fileName << std::endl;
        std::fstream in(fileName.c_str(), std::ios_base::in);
        std::string line;

        std::queue<FiberType> fibers;

        uint64_t startKey = 0;
        while (true) {
            auto data = std::make_shared<std::stringstream>();
            uint64_t count = 0;
            while (std::getline(in, line) && count < 10000) {
                *data << line << '\n';
                ++count;
            }
            if (count == 0) {
                break;
            }
            threaded_populate(connection, fibers, tableName, startKey, data);
            startKey += count;
        }
        while (!fibers.empty()) {
            join(fibers.front());
            fibers.pop();
        }

        std::cout << std::endl << std::endl;
        });
    }
    std::cout << "Done" << std::endl;
    std::cout << '\a';
}

} // namespace tpch
