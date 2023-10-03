%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2023, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(flow_stop_server).

-behaviour(gen_server).

-include("dataflow.hrl").

-export([start_link/1, start_monitor/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).


-define(MIN_IDLE_TIME, 30000).
-define(MIN_IDLE_CHECK_INTERVAL, 15000).

-record(state, {
  parent               :: pid(),
  %% stop_flow_on_idle feature
  stop_on_idle = false :: true | false,
  idle_time = <<"5m">> :: non_neg_integer(),
  idle_check_interval  :: non_neg_integer(),
  idle_since           :: non_neg_integer(),
  idle_check_condition :: undefined | #faxe_lambda{}

}).

%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================

start_link(Args) ->
  gen_server:start_link(?MODULE, [Args], []).

start_monitor(Args) ->
  case gen_server:start(?MODULE, [Args#{parent => self()}], []) of
    {ok, Pid} -> erlang:monitor(process, Pid), {ok, Pid};
    Other -> Other
  end.

init([#{'_idle_time' := IdleTime0, '_stop_when' := StopCond, parent := Parent, graphId := GraphId, nodeId := NodeId}]) ->
  %% stop flow on idle feature
  lager:md([{flow, GraphId}, {comp, NodeId}]),
  IdleTime = max(faxe_time:duration_to_ms(IdleTime0), ?MIN_IDLE_TIME),
  IdleCheckInterval = max(round(IdleTime/5), ?MIN_IDLE_CHECK_INTERVAL),
  case StopCond == undefined of
    true -> erlang:send_after(IdleCheckInterval, self(), check_stop_on_idle);
    false -> ok
  end,

   lager:notice("stop on idle: ~p time: ~p interval: ~p, stop-condition: ~p",["yes", IdleTime, IdleCheckInterval, StopCond]),
  {ok, #state{
    parent = Parent,
    %% stop_flow_on_idle feature
    idle_time = IdleTime,
    idle_check_interval = IdleCheckInterval,
    idle_since = faxe_time:now(),
    idle_check_condition = StopCond
  }}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast({new_item, Item = #data_point{}}, State = #state{}) ->
%%  lager:info("new_item"),
  {noreply, check_stop_condition([Item], reset_idle(State, item))};
handle_cast({new_item, #data_batch{points = Points}}, State = #state{}) ->
  {noreply, check_stop_condition(Points, reset_idle(State, item))};
handle_cast({data_event, Context}, State) ->
%%  lager:info("data_event"),
  {noreply, reset_idle(State, Context)};
handle_cast(_Request, State = #state{}) ->
  {noreply, State}.


handle_info(check_stop_on_idle, State = #state{idle_since = Since, idle_check_interval = Interval,
  idle_time = IdleFor, parent = ParentPid}) ->
  IdleTime = (faxe_time:now()-Since),
%%  lager:notice("idle for ~p secs",[round(IdleTime/1000)]),
  case IdleTime >= IdleFor of
    true ->
      %% I think we do not have to check for message queue length of the process, because, if there were
      %% messages still, we would see that in the handle_info callbacks
      %% (check for any buffer in a node also)
      %% maybe provide a callback (ie: is_idle()) for the module to make sure we really are idle, basically asking:
      %% "is there any hidden work still to be done ?"
      lager:notice("IDLE TIMEOUT !!!"),
      %% tell parent
      ParentPid ! stop_idle;
%%      faxe:stop_task(State#c_state.graph_id, true);
    false ->
      erlang:send_after(Interval, self(), check_stop_on_idle)
  end,
  {noreply, State};
handle_info(stop, State = #state{}) ->
  {stop, whatever, State};
handle_info(_Info, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
%%  lager:info("~p terminate", [?MODULE]),
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


%%%% IDLE STOP FEATURE
reset_idle(State = #state{idle_check_condition = LFun}, _Tag) when is_record(LFun, faxe_lambda) ->
    State;
reset_idle(State, _Tag) ->
    State#state{idle_since = faxe_time:now()}.


check_stop_condition(_PointList, S = #state{idle_check_condition = undefined}) ->
  S;
check_stop_condition([], S = #state{}) ->
  S;
check_stop_condition([DataPoint|R], State = #state{idle_check_condition = LFun}) when is_record(LFun, faxe_lambda) ->
    case faxe_lambda:execute(DataPoint, LFun) of
        true ->
            %% activate stop idle timer
          lager:warning("stop condtion gives TRUE!!"),
            erlang:send_after(State#state.idle_check_interval, self(), check_stop_on_idle),
            State#state{idle_check_condition = undefined};
        false ->
          check_stop_condition(R, State)
    end.