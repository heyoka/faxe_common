%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Sep 2021 19:37
%%%-------------------------------------------------------------------
-module(adaptive_interval).
-author("heyoka").

-include("faxe_common.hrl").

-define(CHANGE_THRESHOLD, 1).

-type adaptive_interval() :: #adapint{}.

%% API
-export_type([adaptive_interval/0]).

-export([in/2, new/0, current/1]).



-spec new() -> adaptive_interval().
new() ->
  A = #adapint{},
  QOpts = faxe_config:get(esq),
  Min = proplists:get_value(deq_min_interval, QOpts, A#adapint.min),
  Max = proplists:get_value(deq_max_interval, QOpts, A#adapint.max),
  StepSize = proplists:get_value(deq_step_size, QOpts, A#adapint.step),
  StartInterval = proplists:get_value(deq_interval, QOpts, round(Max/5)),
  A#adapint{current = StartInterval, min = Min, max = Max, step = StepSize}.

-spec current(adaptive_interval()) -> non_neg_integer().
current(#adapint{current = C}) -> C.

-spec in(hit|miss, adaptive_interval()) -> {non_neg_integer(), adaptive_interval()}.
in(hit, A = #adapint{current = Current, step = Step, min = Min, state_count = C})
    when C >= ?CHANGE_THRESHOLD, (Current - Step) >= Min ->
  N = Current - Step,
  {N, A#adapint{current = N, state_count = 0}};
in(miss, A = #adapint{current = Current, step = Step, max = Max, state_count = C})
    when C >= ?CHANGE_THRESHOLD, (Current + Step) =< Max ->
  N = Current + Step,
  {N, A#adapint{current = N, state_count = 0}};
%% stay where we are
in(_W, A = #adapint{current = Current, state_count = C}) ->
  {Current, A#adapint{state_count = C+1}}.


