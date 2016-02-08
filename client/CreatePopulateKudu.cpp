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

using namespace kudu::client;

void assertOk(kudu::Status status) {
    if (!status.ok()) {
        LOG_ERROR("ERROR from Kudu: %1%", status.message().ToString());
        throw std::runtime_error(status.message().ToString().c_str());
    }
}

template<>
struct TableCreator<kudu::client::KuduSession> {
    KuduSession& session;
    std::unique_ptr<KuduTableCreator> tableCreator;
    KuduSchemaBuilder schemaBuilder;
    TableCreator(KuduSession& session)
        : session(session)
        , tableCreator(session.client()->NewTableCreator())
    {
        tableCreator->num_replicas(1);
    }

    template<class S>
    void operator() (const S& name, type t, bool notNull = true) {
        auto col = schemaBuilder.AddColumn(name);
        switch (t) {
        case type::SMALLINT:
            col->Type(KuduColumnSchema::INT16);
            break;
        case type::INT:
            col->Type(KuduColumnSchema::INT32);
            break;
        case type::BIGINT:
            col->Type(KuduColumnSchema::INT64);
            break;
        case type::FLOAT:
            col->Type(KuduColumnSchema::FLOAT);
            break;
        case type::DOUBLE:
            col->Type(KuduColumnSchema::DOUBLE);
            break;
        case type::TEXT:
            col->Type(KuduColumnSchema::STRING);
            break;
        }
        if (notNull) {
            col->NotNull();
        } else {
            col->Nullable();
        }
    }

    void setPrimaryKey(const std::vector<std::string>& key) {
        schemaBuilder.SetPrimaryKey(key);
    }

    void create(const std::string& name) {
        KuduSchema schema;
        assertOk(schemaBuilder.Build(&schema));

        tableCreator->schema(&schema);
        tableCreator->table_name(name);
        tableCreator->Create();
    }
};

template<>
void createSchema(KuduConnection& client) {
    auto session = client->NewSession();
    assertOk(session->SetFlushMode(kudu::client::KuduSession::MANUAL_FLUSH));
    session->SetTimeoutMillis(60000);
    createTables(*session);
    assertOk(session->Close());
}

template<>
KuduConnection getConnection(std::string &storage, std::string &commitMananger) {
    kudu::client::KuduClientBuilder clientBuilder;
    clientBuilder.add_master_server_addr(storage);
    std::tr1::shared_ptr<kudu::client::KuduClient> client;
    clientBuilder.Build(&client);
    return client;
}

template<>
struct Populator<KuduSession> {
    KuduSession& session;
    std::tr1::shared_ptr<KuduTable> table;
    std::unique_ptr<KuduInsert> ins;
    kudu::KuduPartialRow* row;
    std::vector<std::string> names;
    std::vector<std::string> vals;
    Populator(KuduSession& session, const std::string& tableName)
        : session(session)
    {
        assertOk(session.client()->OpenTable(tableName, &table));
        ins.reset(table->NewInsert());
        row = ins->mutable_row();
    }

    template<class Str>
    void operator() (Str&& name, int16_t val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt16(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, int32_t val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt32(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, int64_t val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt64(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, date d) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetInt64(names.back(), d.value));
    }

    template<class Str>
    void operator() (Str&& name, float val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetFloat(names.back(), val));
    }

    template<class Str>
    void operator() (Str&& name, double val) {
        names.emplace_back(std::forward<Str>(name));
        assertOk(row->SetDouble(names.back(), val));
    }

    template<class Str1, class Str2>
    void operator() (Str1&& name, Str2&& val) {
        names.emplace_back(std::forward<Str1>(name));
        vals.emplace_back(std::forward<Str2>(val));
        assertOk(row->SetString(names.back(), vals.back()));
    }

    void apply(uint64_t k) {
        assertOk(session.Apply(ins.release()));
        ins.reset(table->NewInsert());
        row = ins->mutable_row();
        names.clear();
        vals.clear();
    }

    void flush() {
        assertOk(session.Flush());
    }

    void commit() {
    }
};

template struct Populator<KuduSession>;

template<>
struct string_type<KuduSession> {
    using type = std::string;
};

template struct Populate<KuduSession>;

template<>
void threaded_populate(KuduConnection &client, std::queue<std::thread> &threads,
        std::string &tableName, const uint64_t &startKey, const std::shared_ptr<std::stringstream> &data) {
    if (threads.size() >= 8) {
        threads.front().join();
        threads.pop();
    }
    threads.emplace([&client, &tableName, data, startKey] () {
        auto session = client->NewSession();
        assertOk(session->SetFlushMode(kudu::client::KuduSession::MANUAL_FLUSH));
        session->SetTimeoutMillis(60000);
        Populate<kudu::client::KuduSession> populate(*session);
        populateTable(tableName, startKey, data, populate);
        assertOk(session->Flush());
        assertOk(session->Close());
    });
}

template<>
void join(std::thread &thread) {
    thread.join();
}

template void createSchemaAndPopulate<KuduConnection, std::thread>(std::string &storage, std::string &commitManager, std::string &baseDir);

} // namespace tpch
