-module(riak_kv_entropy_console).
-export([
    handle_aae_enabled/1,
    handle_aae_debug_enabled/1,
    handle_cancel_aae_exchanges/1,
    handle_aae_reset/1,
    handle_aae_status/1
 ]).

-export([
    aae_enable/1,
    aae_disable/1,
    set_debug/2,
    is_aae_debug_enabled/0,
    cancel_exchanges/1,
    aae_reset/1,
    rpc_node_ann/4
 ]).


handle_aae_enabled([]) ->
    get_cluster_aae_enabled();

handle_aae_enabled([Parameter]) ->
    case parse_member_or_boolean(Parameter) of
        {boolean, Switch} -> set_cluster_aae_enabled(Switch);
        {node, Node} -> get_node_aae_enabled(Node);
        {error} -> print_member_or_boolean_error(Parameter)
    end;

handle_aae_enabled([NodeParameter, SwitchParameter]) ->
    case {parse_member_node(NodeParameter), parse_boolean(SwitchParameter)} of 
        {{node,Node},{boolean,Switch}} ->
            set_node_aae_enabled(Node,Switch);
        {{error},_} ->
            print_member_error(NodeParameter);
        {_,{error}} ->
            print_boolean_error(SwitchParameter)
    end.

handle_aae_debug_enabled([]) ->
    get_cluster_aae_debug_enabled();

handle_aae_debug_enabled([Parameter]) ->
    case parse_member_or_boolean(Parameter) of
        {boolean, Switch} -> set_cluster_aae_debug_enabled(Switch);
        {node, Node} -> get_node_aae_debug_enabled(Node);
        {error} -> print_member_or_boolean_error(Parameter)
    end;

handle_aae_debug_enabled([NodeParameter, SwitchParameter]) ->
    case {parse_member_node(NodeParameter), parse_boolean(SwitchParameter)} of 
        {{node,Node},{boolean,Switch}} ->
            set_node_aae_debug_enabled(Node,Switch);
        {{error},_} ->
            print_member_error(NodeParameter);
        {_,{error}} ->
            print_boolean_error(SwitchParameter)
    end.

handle_cancel_aae_exchanges([]) ->
    cluster_cancel_aae_exchanges();

handle_cancel_aae_exchanges([NodeParameter]) ->
    case parse_member_node(NodeParameter) of
        {node, Node} -> node_cancel_aae_exchanges(Node);
        {error} -> print_member_error(NodeParameter)
    end.

handle_aae_reset([]) ->
    %% This command is pretty brutal.  Make 'em promise
    io:format("
Running this command across the entire cluster will result in significant 
cluster activity as the active anti-entropy trees are rebuilt from scratch.  

If you understand and would like to continue anyway, run the command again 
adding the '-f' flag.~n~n",[]);

handle_aae_reset(["-f"]) ->
    cluster_aae_reset();

handle_aae_reset([NodeParameter]) ->
    case parse_member_node(NodeParameter) of
        {node, Node} -> node_aae_reset(Node);
        {error} -> print_member_error(NodeParameter)
    end.

handle_aae_status([]) ->
    get_aae_status().

%% AAE ENABLED

get_cluster_aae_enabled() ->
    print_status_result("AAE Status","Enabled",
        riak_core_util:rpc_every_member_ann(riak_kv_entropy_manager,enabled,[],60000)).

get_node_aae_enabled(Node) ->
    print_status_result("AAE Status","Enabled",
        riak_kv_entropy_console:rpc_node_ann(Node,riak_kv_entropy_manager,enabled,[])).

set_cluster_aae_enabled(Status) ->
    case Status of 
        true ->
            riak_core_util:rpc_every_member_ann(riak_kv_entropy_console,aae_enable,[node()],60000);
        false ->
            riak_core_util:rpc_every_member_ann(riak_kv_entropy_console,aae_disable,[node()],60000);
        _ -> 
            ok  
    end,
    get_cluster_aae_enabled().

set_node_aae_enabled(Node, Status) ->
    case Status of 
        true ->
            riak_kv_entropy_console:rpc_node_ann(Node,riak_kv_entropy_console,aae_enable,[node()]);
        false ->
            riak_kv_entropy_console:rpc_node_ann(Node,riak_kv_entropy_console,aae_disable,[node()]);
        _ -> 
            ok  
    end,
    get_node_aae_enabled(Node).

aae_enable(CallingNode) ->
    send_notice("AAE Enable called from node ~p",[CallingNode]),
    riak_kv_entropy_manager:enable().

aae_disable(CallingNode) ->
    send_notice("AAE Disable called from node ~p",[CallingNode]),
    riak_kv_entropy_manager:disable().

%% AAE DEBUG

get_cluster_aae_debug_enabled() ->
    print_status_result("AAE Debug Status", "Enabled",
        riak_core_util:rpc_every_member_ann(riak_kv_entropy_console,is_aae_debug_enabled,[],60000)).

get_node_aae_debug_enabled(Node) ->
    print_status_result("AAE Debug Status","Enabled",
        riak_kv_entropy_console:rpc_node_ann(Node,riak_kv_entropy_console,is_aae_debug_enabled,[])).

set_cluster_aae_debug_enabled(Status) ->
    case parse_boolean(Status) of 
        {boolean,_} ->
            riak_core_util:rpc_every_member_ann(riak_kv_entropy_console,set_debug,[Status,node()],60000);
        _ -> 
            ok  
    end,
    get_cluster_aae_debug_enabled().

set_node_aae_debug_enabled(Node, Status) ->
    case parse_boolean(Status) of 
        {boolean,_} ->
            riak_kv_entropy_console:rpc_node_ann(Node,riak_kv_entropy_console,set_debug,[Status,node()]);
        _ -> 
            ok  
    end,
    get_node_aae_debug_enabled(Node).

set_debug(Status,CallingNode) ->
    send_notice("AAE Debug logging mode set to ~p from ~p",[Status,CallingNode]),
    riak_kv_entropy_manager:set_debug(Status).

is_aae_debug_enabled() ->
    Response = lager_config:get(loglevel),
    {_,_,Modules} = lists:unzip3(lists:flatten([element(2,Filter) ||{Filter,_,_} <- element(2,Response)])),
    lists:member(riak_kv_entropy_manager,Modules).

%% AAE CANCEL EXCHANGES

cluster_cancel_aae_exchanges() ->
    print_multicall_result(
        riak_core_util:rpc_every_member_ann(riak_kv_entropy_console,cancel_exchanges,[node()],60000),
        "AAE Exchanges were cancelled"
        ).

node_cancel_aae_exchanges(Node) ->
    print_multicall_result(
        riak_kv_entropy_console:rpc_node_ann(Node,riak_kv_entropy_console,cancel_exchanges,[node()]),
        "AAE Exchanges were cancelled"
        ).

cancel_exchanges(CallingNode) ->
    send_notice("AAE Cancel Exchanges called from node ~p",[CallingNode]),
    riak_kv_entropy_manager:cancel_exchanges().


%% AAE RESET
cluster_aae_reset() ->
    print_multicall_result(
        riak_core_util:rpc_every_member_ann(riak_kv_entropy_console,aae_reset,[node()],60000),
        "AAE has been reset"
        ).

node_aae_reset(Node) ->
    print_multicall_result(
        riak_kv_entropy_console:rpc_node_ann(Node,riak_kv_entropy_console,aae_reset,[node()]),
        "AAE has been reset"
        ).

aae_reset(CallingNode) ->
    send_notice("AAE reset called from ~p.",[CallingNode]),
    AAE_Enabled = riak_kv_entropy_manager:enabled(),
    riak_kv_entropy_manager:set_mode(manual),

    case AAE_Enabled of
        false -> riak_kv_entropy_manager:enable();
        _     -> ok 
    end, 

    riak_kv_entropy_manager:cancel_exchanges(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Primaries = riak_core_ring:my_indices(Ring),
    TreePids = [case riak_kv_vnode:hashtree_pid(Index) of
                    {ok, Pid} ->
                        Pid
                end || Index <- Primaries],
    %% Clear trees
    _ = [riak_kv_index_hashtree:clear(Pid) || Pid <- TreePids],

    % Remove AAE directory contents.  TODO: Make non-"os:cmd" version
    %    {ok, DataDir} = application:get_env(riak_kv, anti_entropy_data_dir),
    %    Command = lists:flatten(["rm -rf ",DataDir,"/*"]),
    %    os:cmd(Command),
    % Killing to restart entropy_manager

    erlang:exit(whereis(riak_kv_entropy_manager),"AAE reset"),
    timer:sleep(100),
    riak_kv_entropy_manager:set_mode(automatic),

    case AAE_Enabled of
        false ->     
            riak_kv_entropy_manager:disable();
        _     -> 
            ok 
    end,    

    ok.

%% AAE STATUS
get_aae_status() ->
    get_cluster_aae_enabled(),
    ExchangeInfo = riak_kv_entropy_info:compute_exchange_info(),
    aae_exchange_status(ExchangeInfo),
    io:format("~n"),
    aae_tree_status(),
    io:format("~n"), 
    aae_repair_status(ExchangeInfo).

aae_exchange_status(ExchangeInfo) -> 
    io:format("~s~n", [string:centre(" Exchanges ", 79, $=)]),
    io:format("~-49s  ~-12s  ~-12s~n", ["Index", "Last (ago)", "All (ago)"]),
    io:format("~79..-s~n", [""]),
    [begin
         Now = os:timestamp(),
         LastStr = format_timestamp(Now, LastTS),
         AllStr = format_timestamp(Now, AllTS),
         io:format("~-49b  ~-12s  ~-12s~n", [Index, LastStr, AllStr]),
         ok
     end || {Index, LastTS, AllTS, _Repairs} <- ExchangeInfo],
    ok.

aae_repair_status(ExchangeInfo) ->
    io:format("~s~n", [string:centre(" Keys Repaired ", 79, $=)]),
    io:format("~-49s  ~s  ~s  ~s~n", ["Index",
                                      string:centre("Last", 8),
                                      string:centre("Mean", 8),
                                      string:centre("Max", 8)]),
    io:format("~79..-s~n", [""]),
    [begin
         io:format("~-49b  ~s  ~s  ~s~n", [Index,
                                           string:centre(integer_to_list(Last), 8),
                                           string:centre(integer_to_list(Mean), 8),
                                           string:centre(integer_to_list(Max), 8)]),
         ok
     end || {Index, _, _, {Last,_Min,Max,Mean}} <- ExchangeInfo],
    ok.

aae_tree_status() ->
    TreeInfo = riak_kv_entropy_info:compute_tree_info(),
    io:format("~s~n", [string:centre(" Entropy Trees ", 79, $=)]),
    io:format("~-49s  Built (ago)~n", ["Index"]),
    io:format("~79..-s~n", [""]),
    [begin
         Now = os:timestamp(),
         BuiltStr = format_timestamp(Now, BuiltTS),
         io:format("~-49b  ~s~n", [Index, BuiltStr]),
         ok
     end || {Index, BuiltTS} <- TreeInfo],
    ok.

format_timestamp(_Now, undefined) ->
    "--";
format_timestamp(Now, TS) ->
    riak_core_format:human_time_fmt("~.1f", timer:now_diff(Now, TS)).

%% PRIVATE
parse_member_or_boolean(String) ->
    %% This function is to parse the second parameter into a node, a boolean, or return an error tuple
    %% {boolean, Value}, {node, Node}, {error}
    case parse_boolean(String) of
        {error} ->
            parse_member_node(String);        
        Result ->
            Result
    end.

parse_member_node(String) ->   
    try 
        {ok, MyRing} = riak_core_ring_manager:get_my_ring(),
        Nodes = riak_core_ring:all_members(MyRing),        
        %% Using list_to_existing_atom to prevent buildup of typo'd node names in the atom table        
        NodeAtom = list_to_existing_atom(String),
        case lists:member(NodeAtom, Nodes) of 
            false ->
                {error};
            _ ->
                {node, NodeAtom}
        end
    catch error:badarg ->
        {error}
    end.

parse_boolean(Atom) when is_atom(Atom) ->
    parse_boolean(atom_to_list(Atom));

parse_boolean(String) ->
    case string:to_lower(String) of
        "true" -> {boolean, true};
        "false" -> {boolean, false};
        _ -> {error}
    end.

get_members() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    riak_core_ring:all_members(Ring).

print_member_error(Parameter) ->
    print_parameter_error(Parameter,get_members()).

print_boolean_error(Parameter) ->
    print_parameter_error(Parameter,[true,false]).

print_member_or_boolean_error(Parameter) ->
    print_parameter_error(Parameter, lists:append([true,false], get_members())).

print_parameter_error(BadParameter, GoodParameterList) when is_list(GoodParameterList) ->
    io:format("Invalid Parameter (~s), valid values are:~n~n", [BadParameter]),
    Leader = "  ",
    Trailer = "~n", 
    lists:foreach( 
        fun(Parameter) -> 
            print_parameter(Parameter, Leader, Trailer)
        end, GoodParameterList
    ),
    io:format("~n",[]).

print_parameter(Atom, Leader, Trailer) when is_atom(Atom) ->
    io:format(Leader++"~s"++Trailer,[atom_to_list(Atom)]);

print_parameter(String, Leader, Trailer) when is_list(String) ->
    case io_lib:printable_list(String) of 
        true ->
            io:format(Leader++"~s"++Trailer,[String]);
        _ ->
            ok
    end.

print_multicall_result({Results, Down}, SuccessMessage) ->
    io:format("~n~s on the following nodes:~n", [SuccessMessage]),
    lists:foreach(fun({Node,_}) -> io:format("    ~s~n",[atom_to_list(Node)]) end, Results),
    io:format("The following nodes were not reachable:~n", []),
    lists:foreach(fun(Node) -> io:format("    ~s~n",[atom_to_list(Node)]) end, Down),
    io:format("~n",[]).

print_status_result(Title, ColumnTitle,{Results, Down}) ->
    io:format("~n",[]),
    print_header(Title, ColumnTitle),
    AllResults = lists:sort(lists:append(Results,[ {Node, 'Unreachable'} || Node <- Down])),
    print_table_body(AllResults),
    io:format("~n",[]),
    ok.

print_table_divider() ->
    io:format("~79..-s~n", [""]).

print_table_title(TitleString) ->
    io:format("~s~n", [string:centre(" " ++ TitleString ++ " ", 79, $=)]).

print_node_one_col_header(ColumnName) ->
    io:format("~-49s  ~s~n", ["Node",ColumnName]).

print_node_one_col_body({Node, ColumnValue}) when is_atom(ColumnValue) ->
    io:format("~-49s  ~s~n", [atom_to_list(Node),atom_to_list(ColumnValue)]);

print_node_one_col_body({Node, ColumnValue}) ->
    io:format("~-49s  ~s~n", [atom_to_list(Node),ColumnValue]).

print_header(TitleString, ColumnName) ->
    print_table_title(TitleString),
    print_node_one_col_header(ColumnName),
    print_table_divider().

print_table_body(NodeResultList) ->
    lists:foreach(fun({Node, ColumnValue})-> print_node_one_col_body({Node, ColumnValue}) end,NodeResultList).

rpc_node_ann(Node,M,F,A) ->
    {Success, Fail} = rpc:multicall([Node], M, F, A),
    {[ {Node, Result} || Result <- Success],[Fail]}.

send_notice(FormatString,Parameters) ->
    lager:notice(FormatString,Parameters).
    %io:format("NOTICE: "++FormatString++"~n",Parameters).

