%%% @author Russell Brown <russelldb@basho.com>
%%% @copyright (C) 2014, Russell Brown
%%% @doc
%%% vclock strategy for kv679 test
%%% @end
%%% Created : 19 Aug 2014 by Russell Brown <russelldb@basho.com>

-module(kv679_vclock).

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
    O = merge(Coord, merge(A, B)),
    {O, State#state{coord=O, rep_a=O, rep_b=O}}.

%% Local private
coord_merge(NewC, NewV, undefined) ->
    %% No local copy of key
    {vclock:increment(coord, NewC), [NewV]};
coord_merge(NewC, NewV, {LocalC, LocalV}) ->
    %% Merge with local data
    case vclock:descends(NewC, LocalC) of
        %% the put has seen everything at the coordinator
        true -> {vclock:increment(coord,NewC), [NewV]};
        false ->
            %% concurrent with local data
            C = vclock:increment(coord, vclock:merge([NewC, LocalC])),
            {C, lists:umerge([NewV],  LocalV)}
    end.

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
    case {vclock:descends(Clock1, Clock2), vclock:descends(Clock2, Clock1)} of
        {true, false} ->
            {Clock1, Values1};
        {false, true} ->
            {Clock2, Values2};
        {false, false} ->
            %% Conflict, merge
            {vclock:merge([Clock1, Clock2]),
             lists:umerge(Values1, Values2)};
        {true, true} ->
            %% Must be equal (or what about timestamps?)
            %% Let's do what riak does, and merge
            {vclock:merge([Clock1, Clock2]),
             lists:umerge(Values1, Values2)}
    end.
