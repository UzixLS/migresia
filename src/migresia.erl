%% Copyright (c) 2015, Grzegorz Junka
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are met:
%%
%% Redistributions of source code must retain the above copyright notice,
%% this list of conditions and the following disclaimer.
%% Redistributions in binary form must reproduce the above copyright notice,
%% this list of conditions and the following disclaimer in the documentation
%% and/or other materials provided with the distribution.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%% ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
%% LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
%% CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
%% SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
%% CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
%% ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
%% EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-module(migresia).

-compile({parse_transform, lager_transform}).

-export([start_all_mnesia/0,
    list_nodes/0,
    list_migrations/0,
    ensure_started/1,
    check/1,
    migrate/1,
    rollback/2,
    rollback_last/1]).

-define(TABLES_WAIT, 300000).

%%------------------------------------------------------------------------------

-spec start_all_mnesia() -> ok | {error, any()}.
start_all_mnesia() ->
    lager:info("Starting Mnesia..."),
    case ensure_started(mnesia) of
        ok ->
            ensure_started_on_remotes(list_nodes());
        Err ->
            lager:error(" => Error:~p", [Err]),
            Err
    end.

list_nodes() ->
    mnesia:table_info(schema, disc_copies).

list_migrations() ->
    migresia_migrations:list_migrations().

ensure_started_on_remotes(Nodes) ->
    lager:info("Ensuring Mnesia is running on nodes: ~p", [Nodes]),
    {ResL, BadNodes} = rpc:multicall(Nodes, migresia, ensure_started, [mnesia]),
    handle_err([X || X <- ResL, X /= ok], BadNodes).

handle_err([], []) ->
    lager:info(" => started"),
    ok;
handle_err(Results, Bad) ->
    if Results /= [] -> lager:error(" => Error, received: ~p", [Results]) end,
    if Bad /= [] -> lager:error(" => Error, bad nodes: ~p", [Bad]) end,
    {error, mnesia_not_started}.

-spec ensure_started(atom()) -> ok | {error, any()}.
ensure_started(App) ->
    case application:start(App) of
        ok -> ok;
        {error, {already_started, App}} -> ok;
        {error, _} = Err -> Err
    end.

%%------------------------------------------------------------------------------

-spec check(atom()) -> ok | {error, any()}.
check(App) ->
    case migresia_migrations:list_unapplied_ups(App) of
        [] -> [];
        {error, _} = Err -> Err;
        Loaded -> [X || {X, _} <- Loaded]
    end.

%%------------------------------------------------------------------------------

-type migration_dir() :: file:filename().
-type migration_source() :: atom() | {rel_relative_dir, migration_dir()}.
-type migration_sources() :: migration_source(). %% | [migration_source()].

-spec migrate(migration_sources()) -> ok | {error, any()}.
migrate(Srcs) ->
    try
        ok = migresia_migrations:init_migrations(),
        migrate1(Srcs)
    catch
        throw:{error, _} = Err -> Err
    end.

migrate1(Srcs) ->
    lager:debug("Waiting for tables..."),
    ok = mnesia:wait_for_tables(mnesia:system_info(tables), ?TABLES_WAIT),
    case migresia_migrations:list_unapplied_ups(Srcs) of
        {error, _} = Err -> Err;
        Loaded -> apply_ups(Srcs, Loaded)
    end.

apply_ups(Srcs, Loaded) ->
    %% Load the transform function on all nodes, see:
    %% http://toddhalfpenny.com/2012/05/21/possible-erlang-bad-transform-function-solution/
    rpc:multicall(nodes(), migresia_migrations, list_unapplied_ups, [Srcs]),
    lists:foreach(fun migresia_migrations:execute_up/1, Loaded).

%%------------------------------------------------------------------------------

-spec rollback(migration_sources(), integer()) -> ok | {error, any()}.
rollback(Srcs, Time) ->
    try
        ok = migresia_migrations:init_migrations(),
        rollback1(Srcs, Time)
    catch
        throw:{error, _} = Err -> Err
    end.

rollback1(Srcs, Time) ->
    lager:debug("Waiting for tables..."),
    ok = mnesia:wait_for_tables(mnesia:system_info(tables), ?TABLES_WAIT),
    case migresia_migrations:list_applied_ups(Srcs, Time) of
        {error, _} = Err -> Err;
        Ups -> apply_downs(Srcs, Ups, Time)
    end.

apply_downs(Srcs, Loaded, Time) ->
    %% Load the transform function on all nodes, see:
    %% http://toddhalfpenny.com/2012/05/21/possible-erlang-bad-transform-function-solution/
    rpc:multicall(nodes(), migresia_migrations, list_applied_ups, [Srcs, Time]),
    lists:foreach(fun migresia_migrations:execute_down/1, Loaded).

-spec rollback_last(migration_sources()) -> ok | {error, any()}.
rollback_last(Srcs) -> rollback(Srcs, migresia_migrations:get_ts_before_last()).
