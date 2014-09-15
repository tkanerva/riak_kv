%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%% vclock + dots strategy for kv679 test
%%% @end
%%% Created : 19 Aug 2014 by Russell Brown <russelldb@basho.com>

-module(kv679_dots).

-behaviour(kv679).

-export([init/0, coordinate/2, coordinate/3, read/1, drop_data/1]).

-record(state, {coord,
                rep_a,
                rep_b}).

init() ->
    #state{}.

%% Behaviour API
coordinate(Value, State) ->
    #state{coord=Coord0, rep_a=A0, rep_b=B0} = State,
    Coord = coord_merge(vclock:fresh(), Value, Coord0),
    {A, B} = replicate(A0, B0, Coord),
    State#state{coord=Coord, rep_a=A, rep_b=B}.

coordinate(Value, Ctx, State) ->
    #state{coord=Coord0, rep_a=A0, rep_b=B0} = State,
    Coord = coord_merge(Ctx, Value, Coord0),
    {A, B} = replicate(A0, B0, Coord),
    State#state{coord=Coord, rep_a=A, rep_b=B}.

drop_data(State) ->
    State#state{coord=undefined}.

read(State) ->
    #state{coord=Coord, rep_a=A, rep_b=B} = State,
    {Ctx, Dots}=O = merge(Coord, merge(A, B)),
    Vals = [V || {_Dot, V} <- Dots],
    {{Ctx, Vals}, State#state{coord=O, rep_a=O, rep_b=O}}.

%% Local private
coord_merge(NewC, NewV, undefined) ->
    %% No local copy of key
    C = vclock:increment(coord, NewC),
    Dot = {coord, vclock:get_counter(coord, C)},
    {C, [{Dot, NewV}]};
coord_merge(Ctx, NewV, {LocalC, LocalV}) ->
    %% Merge with local data
    case vclock:descends(Ctx, LocalC) of
        %% the put has seen everything at the coordinator
        true ->
            coord_merge(Ctx, NewV, undefined);
        false ->
            %% concurrent with local data
            %% Drop all dots that the incoming ctx has seen
            RemainingValues = [{Dot, V} || {Dot, V} <- LocalV,
                                           unseen(Dot, Ctx)],
            %% merge clocks and gen new dot
            C = vclock:increment(coord, vclock:merge([Ctx, LocalC])),
            %% Create  a new dot for the new value
            Dot = {coord, vclock:get_counter(coord, C)},
            {C, [{Dot, NewV} | RemainingValues]}
    end.

unseen({Actor, Cntr}, Clock) ->
    vclock:get_counter(Actor, Clock) < Cntr.

replicate(A0, B0, O) ->
    A = merge(A0, O),
    B = merge(B0, O),
    {A, B}.

merge(undefined, undefined) ->
    {[], []};
merge(undefined, B) ->
    B;
merge(A, undefined) ->
    A;
merge({Clock1, Values1}, {Clock2, Values2}) ->
    {Keep, V2Unique} = lists:foldl(fun({Dot, Val}, {Acc, V2}) ->
                                           case {lists:keytake(Dot, 1, V2),
                                                 unseen(Dot, Clock2)} of
                                               {false, true} ->
                                                   %% B does not have, has not seen, so keep
                                                   {[{Dot, Val} | Acc], V2};
                                               {false, false} ->
                                                   %% Already dropped
                                                   %% (B does nothave,
                                                   %% but has seen)
                                                   {Acc, V2};
                                               {{value, {Dot, Val}, V2_2}, _} ->
                                                   %% present in both, keep
                                                   {[{Dot, Val} | Acc], V2_2}
                                           end
                                   end,
                                   {[], Values2},
                                   Values1),
    V2Keep = [{Dot, Val} || {Dot, Val} <- V2Unique,
                            unseen(Dot, Clock1)],
    C = vclock:merge([Clock1, Clock2]),
    {C, lists:sort(V2Keep ++ Keep)}.
