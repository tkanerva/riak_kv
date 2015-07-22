%% -------------------------------------------------------------------
%%
%% riak_kv_pb_index: Expose secondary index queries to Protocol Buffers
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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
%% @doc <p> Special service for riak cs. Fold over objects in buckets.
%% This covers the following request messages:</p>
%%
%% <pre>
%%  40 - RpbCSBucketReq
%% </pre>
%%
%% <p>This service produces the following responses:</p>
%%
%% <pre>
%%  41 - RpbCSBucketResp
%% </pre>
%% @end

-module(riak_kv_pb_csbucket).

-include_lib("riak_pb/include/riak_kv_pb.hrl").
-include("riak_kv_index.hrl").

-behaviour(riak_api_pb_service).

-export([init/0,
         sample_encode/3,
         pb_encode_resp/1,
         decode/2,
         encode/1,
         process/2,
         process_stream/3]).

-record(state, {client, req_id, req, continuation, result_count=0}).

%% @doc init/0 callback. Returns the service internal start
%% state.
-spec init() -> any().
init() ->
    {ok, C} = riak:local_client(),
    #state{client=C}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Decoded = riak_pb_codec:decode(Code, Bin),
    {ok, Decoded}.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode({pre_encoded, Message}) ->
    {ok, Message};
encode(Message) ->
    Encoded = riak_pb_codec:encode(Message),
    {ok, Encoded}.

process(Req=#rpbcsbucketreq{}, State) ->
    #rpbcsbucketreq{start_key=StartKey,
                    start_incl=StartIncl, continuation=Continuation,
                    end_key=EndKey, end_incl=EndIncl} = Req,
    Query = riak_index:to_index_query([
                {field,<<"$bucket">>},
                {start_term, StartKey},
                {start_inclusive, StartIncl},
                {end_term, EndKey},
                {end_inclusive, EndIncl},
                {start_key, StartKey},
                {return_body, true},
                {continuation, Continuation}
                ]),
    maybe_perform_query(Query, Req, State).

%write_bin_to_file(B, FName) ->
%    {ok, F} = file:open(FName, [write]),
%    ok = write_bin_lines(B, F),
%    ok = file:close(F).
%
%write_bin_lines(<<>>, _) ->
%    ok;
%write_bin_lines(<<B, Rest/binary>>, F) ->
%    file:write(F, [integer_to_binary(B), "\n"]),
%    write_bin_lines(Rest, F).

maybe_perform_query({error, Reason}, _Req, State) ->
    {error, {format, Reason}, State};
maybe_perform_query({ok, Query}, Req, State) ->
    #rpbcsbucketreq{type=T, bucket=B, max_results=MaxResults, timeout=Timeout} = Req,
    #state{client=Client} = State,
    Bucket = maybe_bucket_type(T, B),
    Opts = riak_index:add_timeout_opt(Timeout, [{max_results, MaxResults},
                                                {pagination_sort, true}]),
    {ok, ReqId, _FSMPid} = Client:stream_get_index(Bucket, Query, Opts),
    {reply, {stream, ReqId}, State#state{req_id=ReqId, req=Req}}.

%% @doc process_stream/3 callback. Handle streamed responses
process_stream({ReqId, done}, ReqId, State=#state{req_id=ReqId,
                                                  continuation=Continuation,
                                                  req=Req,
                                                  result_count=Count}) ->
    %% Only add the continuation if there may be more results to send
    #rpbcsbucketreq{max_results=MaxResults} = Req,
    Resp = case is_integer(MaxResults) andalso Count >= MaxResults of
               true -> #rpbcsbucketresp{done=1, continuation=Continuation};
               false -> #rpbcsbucketresp{done=1}
           end,
    {done, Resp, State};
process_stream({ReqId, {results, []}}, ReqId, State=#state{req_id=ReqId}) ->
    {ignore, State};
process_stream({ReqId, {results, Results0}}, ReqId, State=#state{req_id=ReqId, req=Req, result_count=Count}) ->
    %%#rpbcsbucketreq{max_results=MaxResults, bucket=Bucket} = Req,
    #rpbcsbucketreq{max_results=MaxResults} = Req,
    dyntrace:p(3),
    Count2 = length(Results0) + Count,
    %% results are {o, Key, Binary} where binary is a riak object
    Continuation = make_continuation(MaxResults, lists:last(Results0), Count2),
    PbResp = pb_encode_resp(Results0),
    dyntrace:p(4),
%%    case app_helper:get_env(riak_kv, debug_hand_coded_pb, false) of
%%        true ->
%%            Results = [encode_result(Bucket, {K, V}) || {o, K, V} <- Results0],
%%            EResp = riak_pb_codec:encode(#rpbcsbucketresp{objects=Results}),
%%            B1 = iolist_to_binary(EResp),
%%            B2 = iolist_to_binary(PbResp),
%%            case B1 == B2 of
%%                true -> ok;
%%                false ->
%%                    write_bin_to_file(B1, "/tmp/original_pb.txt"),
%%                    write_bin_to_file(B2, "/tmp/new_pb.txt"),
%%                    throw(pb_is_fucked)
%%            end;
%%        false ->
%%            ok
%%    end,
    {reply, {pre_encoded, PbResp},
     State#state{continuation=Continuation, result_count=Count2}};
process_stream({ReqId, Error}, ReqId, State=#state{req_id=ReqId}) ->
    {error, {format, Error}, State#state{req_id=undefined}};
process_stream(_,_,State) ->
    {ignore, State}.

pb_encode_resp(Results) ->
    SizedIdxPairs = [pb_encode_idx_pair(Result) || Result <- Results],
    [41, [[10, to_varint(Size), IdxPairList] ||
          {Size, IdxPairList} <- SizedIdxPairs]].

pb_encode_idx_pair({o, K, V}) ->
    dyntrace:p(1),
    {ObjSize, ObjList} = pb_encode_obj(V),
    KSize = size(K),
    KSizeVarint = to_varint(KSize),
    OSizeVarint = to_varint(ObjSize),
    TSize = 2 + varint_size(KSizeVarint) + KSize + varint_size(OSizeVarint) +
        ObjSize,
    Result = {TSize, [10, KSizeVarint, K, 18, OSizeVarint, ObjList]},
    dyntrace:p(2),
    Result.

to_varint(I) when I < 128 ->
    I;
%% Consider special casing 2,3, maybe 4 byte values.
to_varint(I) ->
    to_varint(I, <<>>).

varint_size(I) when is_integer(I) ->
    1;
varint_size(Bin) ->
    size(Bin).

to_varint(I, Bin) when I < 128 ->
    <<Bin/binary, I>>;
to_varint(I, Bin) ->
    SI = (I rem 128) bor 128,
    EI = I bsr 7,
    to_varint(EI, <<Bin/binary, SI>>).

pb_encode_obj(<<53, 1, VCLen:32/integer, VC:VCLen/binary,
                SibCount:32/integer, Sibs/binary>>) ->
    VCLenVarint = to_varint(VCLen),
    L1 = [18, VCLenVarint, VC],
    L1Len = VCLen + 1 + varint_size(VCLenVarint),
    PBSibs = pb_encode_sibs(SibCount, Sibs),
    {TotSibSize, L2} = add_sibs(PBSibs, L1, 0),
    {L1Len+TotSibSize, L2}.

add_sibs([], L, S) ->
    {S, L};
add_sibs([{SibSize, SibIOList}|Rest], L, S) ->
    SibSizeVarint = to_varint(SibSize),
    L2 = [10, SibSizeVarint, SibIOList | L],
    add_sibs(Rest, L2, S + SibSize + varint_size(SibSizeVarint) + 1).

pb_encode_sibs(N, Sibs) ->
    pb_encode_sibs(N, Sibs, []).

pb_encode_sibs(0, _, Acc) ->
    Acc;
pb_encode_sibs(N,
               <<VLen0:32/integer, VBin0:VLen0/binary,
                 MetaLen:32/integer, _MetaBin:MetaLen/binary, Rest/binary>>,
               L0) ->
    <<1, VBin/binary>> = VBin0,
    VLen = VLen0 - 1,
    VLenVarint = to_varint(VLen),
    Len = VLen + varint_size(VLenVarint) + 1,
    L1 = [10, VLenVarint, VBin],
    pb_encode_sibs(N-1, Rest, [{Len, L1} | L0]).

sample_encode(VC, K, V) ->
    riak_pb_codec:encode(#rpbcsbucketresp{
                            objects=[#rpbindexobject{
                                        key=K,
                                        object = #rpbgetresp{
                                                    vclock=VC,
                                                    content=[#rpbcontent {
                                                                value = V
                                                               }
                                                            ]
                                                   }
                                       }
                                    ]}).

%encode_result(_B, {K, V}) when is_binary(V)->
%    dyntrace:p(2, 1),
%    GetResp = riak_pb_kv_codec:encode_get_response(V),
%    dyntrace:p(2, 2),
%    #rpbindexobject{key=K, object=GetResp};
%encode_result(B, {K, V}) ->
%    RObj = riak_object:from_binary(B, K, V),
%    dyntrace:p(3,1),
%    Contents = riak_pb_kv_codec:encode_contents(riak_object:get_contents(RObj)),
%    dyntrace:p(3,2),
%    dyntrace:p(4,1),
%    VClock = pbify_rpbvc(riak_object:vclock(RObj)),
%    dyntrace:p(4,2),
%    GetResp = #rpbgetresp{vclock=VClock, content=Contents},
%    #rpbindexobject{key=K, object=GetResp}.
%
%pbify_rpbvc(Vc) ->
%    riak_object:encode_vclock(Vc).

make_continuation(MaxResults, {o, K, _V}, MaxResults) ->
    riak_index:make_continuation([K]);
make_continuation(_, _, _)  ->
    undefined.

%% Construct a {Type, Bucket} tuple, if not working with the default bucket
maybe_bucket_type(undefined, B) ->
    B;
maybe_bucket_type(<<"default">>, B) ->
    B;
maybe_bucket_type(T, B) ->
    {T, B}.
