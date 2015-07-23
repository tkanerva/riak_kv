-module(riak_kv_encoding_nifs).

-on_load(init/0).
-export([encode_csbucket_resp/1]).

-spec init() -> ok | {error, any()}.
init() ->
    SoName = case code:priv_dir(?MODULE) of
                 {error, bad_name} ->
                     case code:which(?MODULE) of
                         Filename when is_list(Filename) ->
                             filename:join([filename:dirname(Filename),"../priv", "riak_kv_encoding"]);
                         _ ->
                             filename:join("../priv", "riak_kv_encoding")
                     end;
                 Dir ->
                     filename:join(Dir, "riak_kv_encoding")
             end,
    erlang:load_nif(SoName, []).

encode_csbucket_resp(_ObjList) ->
    erlang:nif_error({error, not_loaded}).

