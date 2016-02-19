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
#include "CreatePopulate.hpp"

namespace tpch {

using namespace tell::db;

template<>
struct TableCreator<Transaction> {
    Transaction& tx;
    tell::store::Schema schema;

    TableCreator(Transaction& tx)
        : tx(tx)
        , schema(tell::store::TableType::TRANSACTIONAL)
    {}

    template<class S>
    void operator() (const S& name, type t, bool notNull = true) {
        using namespace tell::store;
        switch (t) {
        case type::SMALLINT:
            schema.addField(FieldType::SMALLINT, name, notNull);
            break;
        case type::INT:
            schema.addField(FieldType::INT, name, notNull);
            break;
        case type::BIGINT:
            schema.addField(FieldType::BIGINT, name, notNull);
            break;
        case type::FLOAT:
            schema.addField(FieldType::FLOAT, name, notNull);
            break;
        case type::DOUBLE:
            schema.addField(FieldType::DOUBLE, name, notNull);
            break;
        case type::TEXT:
            schema.addField(FieldType::TEXT, name, notNull);
            break;
        }
    }

    void create(const std::string& name, double scalingFactor, int partitions) {
        tx.createTable(name, schema);
    }

    void setPrimaryKey(const std::vector<std::string>& fieldNames) {
        // we do not explicitely set the primary key on tell, but we misuse the
        // function here to create addtional counters for all tables as well as
        // an additional index for lineitem and orders
        char firstChar = fieldNames[0][0];
        switch (firstChar) {
        case 'p':
            if (fieldNames[0][1] == 's') {
                tx.createCounter("partsupp_counter");
            } else {
                tx.createCounter("part_counter");
            }
            break;
        case 's':
            tx.createCounter("supplier_counter");
            break;
        case 'c':
            tx.createCounter("customer_counter");
            break;
        case 'o':
            schema.addIndex("o_orderkey_idx",
                std::make_pair(true, std::vector<tell::store::Schema::id_t>{
                    schema.idOf("o_orderkey")
                }));
            tx.createCounter("orders_counter");
            break; 
        case 'l':
            schema.addIndex("l_orderkey_idx",
                    std::make_pair(false, std::vector<tell::store::Schema::id_t>{
                         schema.idOf("l_orderkey")
                    }));
            tx.createCounter("lineitem_counter");
            break;
        case 'n':
            tx.createCounter("nation_counter");
            break;
        case 'r':
            tx.createCounter("region_counter");
            break;
        }
    }
};

template<>
struct Populator<Transaction> {
    Transaction& tx;
    std::unordered_map<crossbow::string, Field> fields;
    tell::db::table_t tableId;
    std::unique_ptr<Counter> counter;

    Populator(Transaction& tx, const crossbow::string& name)
        : tx(tx)
        , counter(new Counter(tx.getCounter(name + "_counter")))
    {
        auto f = tx.openTable(name);
        tableId = f.get();
    }

    template<class Str>
    void operator() (Str&& name, int16_t val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, int32_t val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, int64_t val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, date d) {
        fields.emplace(std::forward<Str>(name), d.value);
    }

    template<class Str>
    void operator() (Str&& name, float val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str>
    void operator() (Str&& name, double val) {
        fields.emplace(std::forward<Str>(name), val);
    }

    template<class Str1, class Str2>
    void operator() (Str1&& name, Str2&& val) {
        fields.emplace(std::forward<Str1>(name), std::forward<Str2>(val));
    }

    void apply() {
        tx.insert(tableId, tell::db::key_t{counter->next()}, fields);
        fields.clear();
    }

    void flush() {
    }

    void commit() {
        tx.commit();
    }
};

template<>
struct string_type<Transaction> {
    using type = crossbow::string;
};

void DBGenBase<TellClient, TellFiber>::createSchema(TellClient& client, double scalingFactor, int partitions) {
    auto schemaFiber = client->clientManager.startTransaction([scalingFactor, partitions] (tell::db::Transaction& tx) {
        createTables(tx, scalingFactor, partitions);
        tx.commit();
    });
    schemaFiber.wait();
}

void DBGenBase<TellClient, TellFiber>::threaded_populate(TellClient &client,
        std::queue<TellFiber> &fibers,
        std::string &tableName, const std::shared_ptr<std::stringstream> data) {
    if (fibers.size() >= 28) {
        fibers.front().wait();
        fibers.pop();
    }
    fibers.emplace(client->clientManager.startTransaction([&tableName, data] (tell::db::Transaction& tx) {
        Populate<tell::db::Transaction> populate(tx);
        populateTable(tableName, data, populate);
        tx.commit();
        std::cout << '.';
        std::cout.flush();
    }));
}

void DBGenBase<TellClient, TellFiber>::join(TellFiber &fiber) {
    fiber.wait();
}

template struct DBGenBase<TellClient, TellFiber>;
template struct DBGenerator<TellClient, TellFiber>;

} // namespace tpch
