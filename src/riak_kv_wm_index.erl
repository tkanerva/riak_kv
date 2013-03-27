%% -------------------------------------------------------------------
%%
%% riak_kv_wm_index - Webmachine resource for running index queries.
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc Resource for running queries on secondary indexes.
%%
%% Available operations:
%%
%% GET /buckets/bucket/indexes/index/op/arg1...
%%   Run an index lookup, return the results as JSON.

-module(riak_kv_wm_index).

%% webmachine resource exports
-export([
         init/1,
         service_available/2,
         forbidden/2,
         malformed_request/2,
         content_types_provided/2,
         encodings_provided/2,
         produce_index_results/2
        ]).

%% @type context() = term()
-record(ctx, {
          client,       %% riak_client() - the store client
          riak,         %% local | {node(), atom()} - params for riak client
          bucket,       %% The bucket to query.
          index_query   %% The query..
         }).

-include_lib("webmachine/include/webmachine.hrl").
-include("riak_kv_wm_raw.hrl").

%% @spec init(proplist()) -> {ok, context()}
%% @doc Initialize this resource.
init(Props) ->
    {ok, #ctx{
       riak=proplists:get_value(riak, Props)
      }}.


%% @spec service_available(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether or not a connection to Riak
%%      can be established. Also, extract query params.
service_available(RD, Ctx=#ctx{riak=RiakProps}) ->
    case riak_kv_wm_utils:get_riak_client(RiakProps, riak_kv_wm_utils:get_client_id(RD)) of
        {ok, C} ->
            {true, RD, Ctx#ctx { client=C }};
        Error ->
            {false,
             wrq:set_resp_body(
               io_lib:format("Unable to connect to Riak: ~p~n", [Error]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

forbidden(RD, Ctx) ->
    {riak_kv_wm_utils:is_forbidden(RD), RD, Ctx}.

%% @spec malformed_request(reqdata(), context()) ->
%%          {boolean(), reqdata(), context()}
%% @doc Determine whether query parameters are badly-formed.
%%      Specifically, we check that the index operation is of
%%      a known type.
malformed_request(RD, Ctx) ->
    %% Pull the params...
    Bucket = list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(bucket, RD))),
    IndexField = list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, wrq:path_info(field, RD))),
    Args1 = wrq:path_tokens(RD),
    Args2 = [list_to_binary(riak_kv_wm_utils:maybe_decode_uri(RD, X)) || X <- Args1],
    ReturnTerms0 = wrq:get_qs_value(?Q_2I_RETURNTERMS, "false", RD),
    ReturnTerms = normalize_boolean(string:to_lower(ReturnTerms0)),
    CanReturnTerms = riak_core_capability:get({riak_kv, '2i_return_terms'}, false),

    case riak_index:to_index_query(IndexField, Args2, (ReturnTerms andalso CanReturnTerms)) of
        {ok, Query} ->
            %% Request is valid.
            NewCtx = Ctx#ctx{
                       bucket = Bucket,
                       index_query = Query
                      },
            {false, RD, NewCtx};
        {error, Reason} ->
            {true,
             wrq:set_resp_body(
               io_lib:format("Invalid query: ~p~n", [Reason]),
               wrq:set_resp_header(?HEAD_CTYPE, "text/plain", RD)),
             Ctx}
    end.

normalize_boolean("false") ->
    false;
normalize_boolean("0") ->
    false;
normalize_boolean("no") ->
    false;
normalize_boolean(_) ->
    true.

%% @spec content_types_provided(reqdata(), context()) ->
%%          {[{ContentType::string(), Producer::atom()}], reqdata(), context()}
%% @doc List the content types available for representing this resource.
%%      "application/json" is the content-type for bucket lists.
content_types_provided(RD, Ctx) ->
    {[{"application/json", produce_index_results}], RD, Ctx}.


%% @spec encodings_provided(reqdata(), context()) ->
%%          {[{Encoding::string(), Producer::function()}], reqdata(), context()}
%% @doc List the encodings available for representing this resource.
%%      "identity" and "gzip" are available for bucket lists.
encodings_provided(RD, Ctx) ->
    {riak_kv_wm_utils:default_encodings(), RD, Ctx}.


%% @spec produce_index_results(reqdata(), context()) -> {binary(), reqdata(), context()}
%% @doc Produce the JSON response to an index lookup.
produce_index_results(RD, Ctx) ->

    case wrq:get_qs_value("stream", "false", RD) of
        "true" ->
            handle_streaming_index_query(RD, Ctx);
        _ ->
            handle_all_in_memory_index_query(RD, Ctx)
    end.

handle_streaming_index_query(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = Ctx#ctx.bucket,
    Query = Ctx#ctx.index_query,

    %% Create a new multipart/mixed boundary
    Boundary = riak_core_util:unique_id_62(),
    CTypeRD = wrq:set_resp_header(
                "Content-Type",
                "multipart/mixed;boundary="++Boundary,
                RD),

    {ok, ReqID} =  Client:stream_get_index(Bucket, Query),
    StreamFun = index_stream_helper(ReqID, Boundary),
    {{stream, {<<>>, StreamFun}}, CTypeRD, Ctx}.

index_stream_helper(ReqID, Boundary) ->
    fun() ->
        receive
            {ReqID, done} ->
                {iolist_to_binary(["\r\n--", Boundary, "--\r\n"]), done};
            {ReqID, {results, Results}} ->
                %% JSONify the results...@TODO k,v pairs?
                JsonKeys1 = {struct, [{?Q_KEYS, Results}]},
                JsonKeys2 = mochijson2:encode(JsonKeys1),
                Body = ["\r\n--", Boundary, "\r\n",
                        "Content-Type: application/json\r\n\r\n",
                        JsonKeys2],
                {iolist_to_binary(Body), index_stream_helper(ReqID, Boundary)};
            {ReqID, Error} ->
                lager:error("Error in index wm: ~p", [Error]),
                Body = ["\r\n--", Boundary, "\r\n",
                        "Content-Type: text/plain\r\n\r\n",
                        "there was an error..."],
                {iolist_to_binary(Body), done}
        after 60000 ->
            {error, timeout}
        end
    end.

handle_all_in_memory_index_query(RD, Ctx) ->
    Client = Ctx#ctx.client,
    Bucket = Ctx#ctx.bucket,
    Query = Ctx#ctx.index_query,

    %% Do the index lookup...
    case Client:get_index(Bucket, Query) of
        {ok, Results} ->
            %% JSONify the results...@TODO what about K,V pairs?
            JsonKeys1 = {struct, [{?Q_KEYS, Results}]},
            JsonKeys2 = mochijson2:encode(JsonKeys1),
            {JsonKeys2, RD, Ctx};
        {error, Reason} ->
            {{error, Reason}, RD, Ctx}
    end.


