%% -------------------------------------------------------------------
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

%% @doc This module uses the riak_core_vnode_worker behavior to perform
%% different riak_kv tasks asynchronously.

-module(riak_kv_worker).
-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-include_lib("riak_kv_vnode.hrl").

-record(state, {index :: partition()}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Initialize the worker. Currently only the VNode index
%% parameter is used.
init_worker(VNodeIndex, _Args, _Props) ->
    {ok, #state{index=VNodeIndex}}.

%-define(PROF_QUERY, 1).

-ifdef(PROF_QUERY).
fakeResp() ->
    [{<<16,0,0,0,3,12,183,128,8,16,0,0,0,2,18,163,217,109,244,59,69,150,199,107,180,219,128,8,18,163,217,109,244,59,69,150,199,107,180,219,128,8,16,0,0,0,3,18,179,88,109,182,155,101,230,98,8,18,185,217,110,86,155,45,206,176,8,10,0,0,0,200>>,<<53,1,0,0,0,27,131,108,0,0,0,1,104,2,109,0,0,0,1,0,104,2,97,1,110,5,0,206,70,77,208,14,106,0,0,0,1,0,0,0,86,2,135,168,109,121,102,97,109,105,108,121,167,102,97,109,105,108,121,49,168,109,121,115,101,114,105,101,115,167,115,101,114,105,101,115,88,164,116,105,109,101,100,165,109,121,105,110,116,1,165,109,121,98,105,110,165,116,101,115,116,49,167,109,121,102,108,111,97,116,203,63,240,0,0,0,0,0,0,166,109,121,98,111,111,108,195,0,0,0,52,0,0,5,177,0,0,188,142,0,14,180,171,22,49,116,113,48,115,113,82,122,121,107,49,51,88,103,81,112,73,108,98,54,108,57,0,0,0,0,4,1,100,100,108,0,0,0,4,0,131,97,1>>}].
-endif.

-ifdef(PROF_QUERY).
handleWorkProf(_FinishFun, _FoldFun, Sender) ->
    Monitor = riak_core_vnode:monitor(Sender),
    Bucket = {<<"GeoCheckin">>,<<"GeoCheckin">>},
    Items = fakeResp(),
    riak_core_vnode:reply(Sender, {{self(), Monitor}, Bucket, Items}),
    riak_core_vnode:reply(Sender, done).
-else.
handleWorkProf(FinishFun, FoldFun, _Sender) ->
    FinishFun(FoldFun()).
-endif.

%% @doc Perform the asynchronous fold operation.
handle_work({fold, FoldFun, FinishFun}, Sender, State) ->
    try
	handleWorkProf(FinishFun, FoldFun, Sender)
    catch
        throw:receiver_down -> ok;
        throw:stop_fold     -> ok;
        throw:PrematureAcc  -> FinishFun(PrematureAcc)
    end,
    {noreply, State}.
