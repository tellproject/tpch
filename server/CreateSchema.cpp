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
#include "CreateSchema.hpp"
#include <telldb/Transaction.hpp>

namespace tpch {

using namespace tell;

namespace {

void createRegion(db::Transaction& transaction) {
    // Primary key: (r_regionkey)
    //              ( 2 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);  // TODO: change to NON_TRANSACTIONAL once it is supported properly
    schema.addField(store::FieldType::SMALLINT, "r_regionkey", true);
    schema.addField(store::FieldType::TEXT, "r_name", true);
    schema.addField(store::FieldType::TEXT, "r_comment", true);
    transaction.createTable("region", schema);
}

void createNation(db::Transaction& transaction) {
    // Primary key: (r_nationkey)
    //              ( 2 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);  // TODO: change to NON_TRANSACTIONAL once it is supported properly
    schema.addField(store::FieldType::SMALLINT, "n_nationkey", true);
    schema.addField(store::FieldType::TEXT, "n_name", true);
    schema.addField(store::FieldType::SMALLINT, "n_regionkey", true);
    schema.addField(store::FieldType::TEXT, "n_comment", true);
    transaction.createTable("nation", schema);
}

void createPart(db::Transaction& transaction) {
    // Primary key: (p_partkey)
    //              ( 4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);  // TODO: change to NON_TRANSACTIONAL once it is supported properly
    schema.addField(store::FieldType::INT, "p_partkey", true);
    schema.addField(store::FieldType::TEXT, "p_name", true);
    schema.addField(store::FieldType::TEXT, "p_mfgr", true);
    schema.addField(store::FieldType::TEXT, "p_brand", true);
    schema.addField(store::FieldType::TEXT, "p_type", true);
    schema.addField(store::FieldType::INT, "p_size", true);
    schema.addField(store::FieldType::TEXT, "p_container", true);
    schema.addField(store::FieldType::DOUBLE, "p_retailprice", true);  //numeric (15,2)
    schema.addField(store::FieldType::TEXT, "p_comment", true);
    transaction.createTable("part", schema);
}

void createSupplier(db::Transaction& transaction) {
    // Primary key: (s_suppkey)
    //              ( 4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);  // TODO: change to NON_TRANSACTIONAL once it is supported properly
    schema.addField(store::FieldType::INT, "s_suppkey", true);
    schema.addField(store::FieldType::TEXT, "s_name", true);
    schema.addField(store::FieldType::TEXT, "s_address", true);
    schema.addField(store::FieldType::SMALLINT, "s_nationkey", true);
    schema.addField(store::FieldType::TEXT, "s_phone", true);
    schema.addField(store::FieldType::DOUBLE, "s_acctbal", true);  //numeric (15,2)
    schema.addField(store::FieldType::TEXT, "s_comment", true);
    transaction.createTable("supplier", schema);
}

void createPartSupp(db::Transaction& transaction) {
    // Primary key: (ps_partkey, ps_suppkey)
    //              ( 4 b ,     4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);  // TODO: change to NON_TRANSACTIONAL once it is supported properly
    schema.addField(store::FieldType::INT, "ps_partkey", true);
    schema.addField(store::FieldType::INT, "ps_suppkey", true);
    schema.addField(store::FieldType::INT, "ps_availqty", true);
    schema.addField(store::FieldType::DOUBLE, "ps_supplycost", true);  //numeric (15,2)
    schema.addField(store::FieldType::TEXT, "ps_comment", true);
    transaction.createTable("partsupp", schema);
}

void createCustomer(db::Transaction& transaction) {
    // Primary key: (c_custkey)
    //              ( 4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "c_custkey", true);
    schema.addField(store::FieldType::TEXT, "c_name", true);
    schema.addField(store::FieldType::TEXT, "c_address", true);
    schema.addField(store::FieldType::SMALLINT, "c_nationkey", true);
    schema.addField(store::FieldType::TEXT, "c_phone", true);
    schema.addField(store::FieldType::DOUBLE, "c_acctbal", true);       //numeric (15,2)
    schema.addField(store::FieldType::TEXT, "c_mktsegment", true);
    schema.addField(store::FieldType::TEXT, "c_comment", true);
    transaction.createTable("customer", schema);
}

void createOrders(db::Transaction& transaction) {
    // Primary key: (o_orderkey)
    //              ( 4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "o_orderkey", true);
    schema.addField(store::FieldType::INT, "o_custkey", true);
    schema.addField(store::FieldType::TEXT, "o_orderstatus", true);        // char (1)
    schema.addField(store::FieldType::DOUBLE, "o_totalprice", true);       //numeric (15,2)
    schema.addField(store::FieldType::BIGINT, "o_orderdate", true);        //datetime (millisecs since 1970)
    schema.addField(store::FieldType::TEXT, "o_orderpriority", true);
    schema.addField(store::FieldType::TEXT, "o_clerk", true);
    schema.addField(store::FieldType::INT, "o_shippriority", true);
    schema.addField(store::FieldType::TEXT, "o_comment", true);
    transaction.createTable("orders", schema);
}

void createLineItems(db::Transaction& transaction) {
    // Primary key: (l_orderkey, l_linenumber)
    //              ( 4 b ,     4 b )
    store::Schema schema(store::TableType::TRANSACTIONAL);
    schema.addField(store::FieldType::INT, "l_orderkey", true);
    schema.addField(store::FieldType::INT, "l_partkey", true);
    schema.addField(store::FieldType::INT, "l_suppkey", true);
    schema.addField(store::FieldType::INT, "l_linenumber", true);
    schema.addField(store::FieldType::DOUBLE, "l_quantity", true);        //numeric (15,2)
    schema.addField(store::FieldType::DOUBLE, "l_extendedprice", true);   //numeric (15,2)
    schema.addField(store::FieldType::DOUBLE, "l_discount", true);        //numeric (15,2)
    schema.addField(store::FieldType::DOUBLE, "l_tax", true);             //numeric (15,2)
    schema.addField(store::FieldType::TEXT, "l_returnflag", true);    // char (1)
    schema.addField(store::FieldType::TEXT, "l_linestatus", true);    // char (1)
    schema.addField(store::FieldType::BIGINT, "l_shipdate", true);        //datetime (nanosecs since 1970)
    schema.addField(store::FieldType::BIGINT, "l_commitdate", true);      //datetime (nanosecs since 1970)
    schema.addField(store::FieldType::BIGINT, "l_receiptdate", true);     //datetime (nanosecs since 1970)
    schema.addField(store::FieldType::TEXT, "l_shipinstruct", true);
    schema.addField(store::FieldType::TEXT, "l_shipmode", true);
    schema.addField(store::FieldType::TEXT, "l_comment", true);
    transaction.createTable("lineitem", schema);
}

} // anonymouse namespace

void createSchema(tell::db::Transaction& transaction) {
    createRegion(transaction);
    createNation(transaction);
    createPart(transaction);
    createSupplier(transaction);
    createPartSupp(transaction);
    createCustomer(transaction);
    createOrders(transaction);
    createLineItems(transaction);
}

} // namespace tpch
