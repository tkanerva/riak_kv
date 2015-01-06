%%% @author    Gordon Guthrie
%%% @copyright (C) 2014, basho
%%% @doc       a runner module for developing riak local index
%%%
%%% @end
%%% Created : 30 Dec 2014 by gguthrie@basho.or

-module(riak_kv_li_runner).

-include("riak_kv_wm_raw.hrl").

%% runner api
-export([
	 put/0,
	 puts/0,
	 get/0,
	 setup_testing/0,
	 show_testing_bucket_props/0
	]).

-export([
	 is_li_index_valid/2
	]).

-include("riak_kv_index.hrl").

-define(BUCKET,      <<"li_testing">>).
-define(CLIENTID,    <<"li_client">>).
-define(MAINKEY,     <<"main_key_bin">>).
-define(SUBKEY1,     <<"subidx1_bin">>).
-define(LI_PROP_KEY, li_index).
-define(RANGEKEY,    <<"key">>).

get() ->
    Client = riak_client:new(node(), ?CLIENTID),
    Query = #riak_kv_li_index_v1{key = ?RANGEKEY},
    io:format("getting a range query with ~p~n", [Query]),
    Opts = [{timeout, 1000}],
    Ret = riak_client:get_index(?BUCKET, Query, Opts, Client),
    io:format("riak_kv_li_runner:get returns ~p~n", [Ret]),
    ok.

puts() ->
    MakeArgsFun = fun(N) ->
			  Int  = integer_to_list(N),
			  Val  = list_to_binary("val" ++ Int),
			  Subs = [{?SUBKEY1, list_to_binary("subkey" ++ Int)}],
			  {?RANGEKEY, Val, Subs}
		  end,
    Args = [MakeArgsFun(X) || X <- lists:seq(1, 10)],
    [ok = put(X, Y, Z) || {X, Y, Z} <- Args],	
    ok.

put() ->
    put(<<"key1">>, <<"val1">>, [{?SUBKEY1, <<"subkey1">>}]).

put(Key, Val, Subkeys) ->
    io:format("Key is ~p Val is ~p SubKeys is ~p~n", [Key, Val, Subkeys]),
    RObj = riak_object:new(?BUCKET, Key, Val),
    MetaData = riak_object:get_metadata(RObj),
    Key = riak_object:key(RObj),
    {MasterKey, Indices} = riak_object:make_li_index(Key, ?MAINKEY, Subkeys), 
    case is_li_index_valid(?BUCKET, Indices) of
	false -> exit("trying to write a value with a duff index");
	true  -> NewMetaD0 = dict:store(?MD_LI_IDX, MasterKey, MetaData),
		 NewMetaD1 = dict:store(?MD_INDEX, Indices, NewMetaD0),
		 NewRObj = riak_object:apply_updates(riak_object:update_metadata(RObj, NewMetaD1)),
		 Options = [],
		 Client = riak_client:new(node(), ?CLIENTID),
		 ok = riak_client:put(NewRObj, Options, Client)
    end.

setup_testing() ->
    Client = riak_client:new(node(), ?CLIENTID),
    BucketProps = [{?LI_PROP_KEY, {?MAINKEY, [?SUBKEY1]}}],
    ok = riak_client:set_bucket(?BUCKET, BucketProps, Client).

show_testing_bucket_props() ->
    Client = riak_client:new(node(), ?CLIENTID),
    _Props = riak_client:get_bucket(?BUCKET, Client).

is_li_index_valid(Bucket, Indices) ->
    Client = riak_client:new(node(), ?CLIENTID),
    Props = riak_client:get_bucket(Bucket, Client),    
    case lists:keyfind(?LI_PROP_KEY, 1, Props) of
	false             -> io:format("no li index set~n"),
			     false;
	{?LI_PROP_KEY, I} -> do_indexes_match(Indices, I)
    end.
	    
do_indexes_match(Indices, {Main, Subs}) ->
    {Idxs, _} = lists:unzip(Indices),
    io:format("Idxs is ~p and ~p~n", [Idxs, [Main | Subs]]),
    lists:sort(Idxs) =:= lists:sort([Main | Subs]).

	    
