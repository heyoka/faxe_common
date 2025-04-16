%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(connection_registry).

-behaviour(gen_server).

-export([start_link/0, connected/0, disconnected/0, connecting/0, reg/4,
  get_connection/1, get_connection_msgs/1, disconnected_ok/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3]).

-include("faxe_common.hrl").

-define(SERVER, ?MODULE).
%%
%% status: 0 = disconnected, 1 = connected, 2 = connecting
-record(conreg, {
  status = 0,
  connected = false,
  flowid,
  nodeid,
  peer,
  port,
  conn_type,
  meta

}).

-record(state, {
  clients = [],
  flow_conns = #{},
  flows = []
}).

-define(STATUS_DISCONNECTED_OK,   3).
-define(STATUS_CONNECTING,        2).
-define(STATUS_CONNECTED,         1).
-define(STATUS_DISCONNECTED,      0).
%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
reg({_FlowId, _NodeId} = Id, Peer, Port, Type) ->
  ?SERVER ! {reg, self(), Id, Peer, Port, Type}.

connecting() ->
  ?SERVER ! {connecting, self()}.

connected() ->
  ?SERVER ! {connected, self()}.

disconnected() ->
  ?SERVER ! {disconnected, self()}.

disconnected_ok() ->
  ?SERVER ! {disconnected_ok, self()}.

start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
  {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
  {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
  {noreply, State}.

handle_info({reg, Client, {FlowId, NodeId} = _Id, Peer, Port, Type} = _R, State=#state{clients = Clients}) ->
  erlang:monitor(process, Client),
  Peer1 =
  case is_list(Peer) of
    true -> list_to_binary(Peer);
    false -> Peer
  end,
  write(Client, #conreg{peer = Peer1, port = Port, flowid = FlowId, nodeid = NodeId, conn_type = Type}),
  {noreply, State#state{clients = [Client|Clients]}};
handle_info({connecting, Client} = _R, State) ->
  maybe_new_status(?STATUS_CONNECTING, Client),
  {noreply, State};
handle_info({connected, Client } = _R, State) ->
  maybe_new_status(?STATUS_CONNECTED, Client),
  {noreply, State};
handle_info({disconnected, Client} = _R, State) ->
  maybe_new_status(?STATUS_DISCONNECTED, Client),
  {noreply, State};
handle_info({disconnected_ok, Client} = _R, State) ->
  maybe_new_status(?STATUS_DISCONNECTED_OK, Client),
  {noreply, State};
handle_info({'DOWN', _Mon, process, Pid, _Info}, State = #state{clients = Clients}) ->
  case lists:member(Pid, Clients) of
    true ->
      Con = get_connection(Pid),
      remove_node_entries(Pid),
      publish(Con#conreg{status = 0, connected = false});
    false ->
      lager:notice("flow is down but not watched?: ~p",[Pid])
  end,
  {noreply, State#state{clients = lists:delete(Pid, Clients)}};
handle_info(_Info, State = #state{}) ->
  {noreply, State}.

terminate(_Reason, _State = #state{}) ->
  ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
maybe_new_status(NewStatus, Client) ->
  Con = get_connection(Client),
  case Con#conreg.status of
    NewStatus -> ok;
    _ -> out(Client, Con#conreg{connected = con_from_status(NewStatus), status = NewStatus})
  end.

con_from_status(?STATUS_CONNECTED) -> true;
con_from_status(_) -> false.


-spec get_connection(pid()) -> #conreg{}.
get_connection(Pid) ->
  get_connection(Pid, false).

get_connection(Pid, Strict) ->
  case ets:lookup(node_connections, Pid) of
    [{Pid, #conreg{}=C}] ->
      C;
    _ -> %% Client not found !!!
      case Strict of
        true -> undefined;
        false -> #conreg{connected = false}
      end
  end.

%% get connection messages for a list of graph node pids
get_connection_msgs(Pids) when is_list(Pids) ->
  Conns0 = [get_connection(Pid, true) || Pid <- Pids],
  Conns = [C || C <- Conns0, C /= undefined],
  [build_datapoint(Conn) || Conn=#conreg{} <- Conns].


remove_node_entries(Pid) ->
  ets:match_delete(node_connections, {Pid, '_'}).

out(_Client, #conreg{flowid = undefined, nodeid = undefined}) ->
  ok;
out(Client, Con=#conreg{}) ->
  write(Client, Con),
  publish(Con).

write(Client, Con=#conreg{}) ->
  ets:insert(node_connections, {Client, Con}).

publish(Conn = #conreg{flowid = FId, nodeid = NId}) ->
  P = build_datapoint(Conn),
  catch gen_event:notify(conn_status, {{FId, NId}, P}).

build_datapoint(#conreg{connected = Connected, status = Status, flowid = FId,
  nodeid = NId, peer = Peer, port = Port, conn_type = Type}) ->
  Fields = #{
    <<"connected">> => Connected,
    <<"status">> => Status,
    <<"flow_id">> => FId,
    <<"node_id">> => NId,
    <<"peer">> => Peer,
    <<"port">> => Port,
    <<"conn_type">> => Type
  },
  #data_point{ts = faxe_time:now(), fields = Fields}.
