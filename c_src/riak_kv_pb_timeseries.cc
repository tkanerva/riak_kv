// -------------------------------------------------------------------
//
// NIF version of protocol buffer encode and decode for certain objects
//
// Copyright (c) 2011-2015 Basho Technologies, Inc. All Rights Reserved.
//
// This file is provided to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain
// a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// -------------------------------------------------------------------

#include <stdint.h>
#include <string.h>
#include <arpa/inet.h>
#include "erl_nif.h"
#include "erl_driver.h"

//
// NIF functions
//
ERL_NIF_TERM EncodePartitionKey(ErlNifEnv* Env, int Argc, const ERL_NIF_TERM Argv[]);
ERL_NIF_TERM EncodeLocalKey(ErlNifEnv* Env, int Argc, const ERL_NIF_TERM Argv[]);

static ErlNifFunc function_map[] =
{
    {"get_partition_key", 3, EncodePartitionKey},
    {"get_local_key",     3, EncodeLocalKey}
};

static int
OnLoad(
    ErlNifEnv* Env,
    void** PrivData,
    ERL_NIF_TERM LoadInfo)
{
    return 0;
}


ERL_NIF_INIT(riak_kv_pb_timeseries, function_map, &OnLoad, NULL, NULL, NULL);


ERL_NIF_TERM
EncodePartitionKey(
    ErlNifEnv* Env,
    int Argc,
    const ERL_NIF_TERM Argv[])
{
    ErlNifBinary family, series, key;
    ErlNifSInt64 stamp;
    const ERL_NIF_TERM * tuple;
    int tuple_count, ret_val;
    size_t key_size;
    char * ptr;
    long temp, net_temp;

    ret_val=enif_get_tuple(Env, Argv[1], &tuple_count, &tuple);

    ret_val=enif_inspect_binary(Env, tuple[0], &family);
    ret_val=enif_inspect_binary(Env, tuple[1], &series);
    ret_val=enif_get_int64(Env, tuple[2], &stamp);
    stamp=(stamp / 900000) * 900000;
    temp=(long)stamp;
    net_temp=htonl(temp);
    temp=0;

    key_size=family.size + series.size + 3 + 8;

    ret_val=enif_alloc_binary(key_size, &key);
    ptr=(char *)key.data;
    *ptr=(char)family.size;
    ++ptr;
    memcpy(ptr, family.data, family.size);
    ptr+=family.size;
    *ptr=(char)series.size;
    ++ptr;
    memcpy(ptr, series.data, series.size);
    ptr+=series.size;
    *ptr=(char)8;
    ++ptr;
    memcpy(ptr, &temp, 4);
    ptr+=4;
    memcpy(ptr, &net_temp, 4);

    return(enif_make_binary(Env, &key));

}   // EncodePartitionKey



ERL_NIF_TERM
EncodeLocalKey(
    ErlNifEnv* Env,
    int Argc,
    const ERL_NIF_TERM Argv[])
{
    ErlNifBinary family, series, key;
    ErlNifSInt64 stamp;
    const ERL_NIF_TERM * tuple;
    int tuple_count, ret_val;
    size_t key_size;
    char * ptr;
    long temp, net_temp;

    ret_val=enif_get_tuple(Env, Argv[1], &tuple_count, &tuple);

    ret_val=enif_inspect_binary(Env, tuple[0], &family);
    ret_val=enif_inspect_binary(Env, tuple[1], &series);
    ret_val=enif_get_int64(Env, tuple[2], &stamp);
    temp=(long)stamp;
    net_temp=htonl(temp);
    temp=0;

    key_size=family.size + series.size + 3 + 8;

    ret_val=enif_alloc_binary(key_size, &key);
    ptr=(char *)key.data;
    *ptr=(char)family.size;
    ++ptr;
    memcpy(ptr, family.data, family.size);
    ptr+=family.size;
    *ptr=(char)series.size;
    ++ptr;
    memcpy(ptr, series.data, series.size);
    ptr+=series.size;
    *ptr=(char)8;
    ++ptr;
    memcpy(ptr, &temp, 4);
    ptr+=4;
    memcpy(ptr, &net_temp, 4);

    return(enif_make_binary(Env, &key));

}   // EncodeLocalKey
