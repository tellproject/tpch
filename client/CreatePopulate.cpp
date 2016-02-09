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

    void create(const std::string& name) {
        tx.createTable(name, schema);
    }

    void setPrimaryKey(const std::vector<std::string>& fieldNames) {
        // we do not explicitely set the primary key on tell, but we misuse the
        // function here to create an additional index and counter for lineitem
        // and orders
        if (fieldNames[0][0] == 'o') {
            schema.addIndex("o_orderkey_idx",
                    std::make_pair(true, std::vector<tell::store::Schema::id_t>{
                         schema.idOf("o_orderkey")
                    }));
            tx.createCounter("order_idx_counter");
        }
        if (fieldNames[0][0] == 'l') {
            schema.addIndex("l_orderkey_idx",
                    std::make_pair(false, std::vector<tell::store::Schema::id_t>{
                         schema.idOf("l_orderkey")
                    }));
            tx.createCounter("lineitem_idx_counter");
        }
    }
};

template<>
struct Populator<Transaction> {
    Transaction& tx;
    std::unordered_map<crossbow::string, Field> fields;
    tell::db::table_t tableId;
    const bool isOrderTable;
    const bool isLineitemTable;
    std::unique_ptr<Counter> counter;

    Populator(Transaction& tx, const crossbow::string& name)
        : tx(tx)
        , isOrderTable(name == "orders")
        , isLineitemTable(name == "lineitem")
        , counter(isOrderTable ? new Counter (tx.getCounter("order_idx_counter")) :
                (isLineitemTable ? new Counter (tx.getCounter("lineitem_idx_counter")) : nullptr))
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

    void apply(uint64_t key) {
        if (isOrderTable || isLineitemTable)
            tx.insert(tableId, tell::db::key_t{counter->next()}, fields);
        else
            tx.insert(tableId, tell::db::key_t{key}, fields);
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

void DBGenBase<TellClient, TellFiber>::createSchema(TellClient& client) {
    auto schemaFiber = client->clientManager.startTransaction([] (tell::db::Transaction& tx) {
        createTables(tx);
        tx.commit();
    });
    schemaFiber.wait();
}

TellClient DBGenBase<TellClient, TellFiber>::getClient(std::string &storage, std::string &commitManager) {
    tell::store::ClientConfig clientConfig;
    clientConfig.tellStore = tell::store::ClientConfig::parseTellStore(storage);
    clientConfig.commitManager = crossbow::infinio::Endpoint(crossbow::infinio::Endpoint::ipv4(), commitManager.c_str());
    clientConfig.numNetworkThreads = 7;
    return std::make_shared<TellClientImpl>(std::move(clientConfig));
}

void DBGenBase<TellClient, TellFiber>::threaded_populate(TellClient &client,
        std::queue<TellFiber> &fibers,
        std::string &tableName, const uint64_t &startKey, const std::shared_ptr<std::stringstream> &data) {
    if (fibers.size() >= 28) {
        fibers.front().wait();
        fibers.pop();
    }
    fibers.emplace(client->clientManager.startTransaction([&tableName, data, startKey] (tell::db::Transaction& tx) {
        Populate<tell::db::Transaction> populate(tx);
        populateTable(tableName, startKey, data, populate);
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
