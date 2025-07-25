%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% some utility functions
%%% @end
%%% Created : 09. Dec 2019 09:57
%%%-------------------------------------------------------------------
-module(faxe_util).
-author("heyoka").

-include("faxe_common.hrl").

%% API
-export([
   uuid_string/0, round_float/2,
   prefix_binary/2,
   host_with_protocol/1, host_with_protocol/2
   , decimal_part/2, check_select_statement/1,
   clean_query/1, stringize_lambda/1,
   bytes_from_words/1, local_ip_v4/0,
   ip_to_bin/1, device_name/0, proplists_merge/2,
   levenshtein/2, build_topic/2, build_topic/1,
   to_bin/1, flip_map/1,
   get_erlang_version/0, get_device_name/0,
   bytes/1, to_num/1, save_binary_to_atom/1,
   to_rkey/1, random_latin_binary/2, random_latin_binary/1,
   type/1, to_list/1,
   check_mqtt_topic/1, check_publisher_mqtt_topic/1, mod/2, subtopic/2]).

-define(HTTP_PROTOCOL, <<"http://">>).

%% @doc returns a new map where the keys became the values and vice versa
-spec flip_map(map()) -> map().
flip_map(Map) when map_size(Map) == 0 ->
   #{};
flip_map(Map) when is_map(Map) ->
   maps:fold(fun(K, V, M) -> M#{V => K} end, #{}, Map).

%% @doc get a uuid v4 binary string
-spec uuid_string() -> binary().
uuid_string() ->
   uuid:uuid_to_string(uuid:get_v4(strong)).

%% @doc round a floating point number with a given precision
-spec round_float(float(), non_neg_integer()) -> float().
round_float(Float, Precision) when is_float(Float), is_integer(Precision) ->
%%   list_to_float(io_lib:format("~.*f",[Precision, Float])).
   list_to_float(float_to_list(Float, [compact, {decimals, Precision}])).

%% @doc get the decimal part of a float as an integer
%% note that rounding occurs if there are more decimals in the float than given by the parameter
%% note also that trailing zeros will be truncated
-spec decimal_part(float(), non_neg_integer()) -> integer().
decimal_part(Float, Decimals) when is_float(Float), is_integer(Decimals) ->
   [_, Dec] =
      string:split(
         float_to_binary(Float, [compact, {decimals, Decimals}]),
         <<".">>
      ),
   binary_to_integer(Dec).

%% @doc
%% check if a given protocol prefix is present, if not prepend it
-spec host_with_protocol(binary()) -> binary().
host_with_protocol(Host) when is_binary(Host) ->
   prefix_binary(Host, ?HTTP_PROTOCOL).
host_with_protocol(Host, Protocol) when is_binary(Host) ->
   prefix_binary(Host, Protocol).

%% @doc check if a given binary string is a prefix, if not prepend it
-spec prefix_binary(binary(), binary()) -> binary().
prefix_binary(Bin, Prefix) when is_binary(Bin), is_binary(Prefix) ->
   Len = string:length(Prefix),
   case binary:longest_common_prefix([Prefix, Bin]) of
      Len -> Bin;
      _ -> <<Prefix/binary, Bin/binary>>
   end.

%% @doc clean an sql statement
-spec clean_query(binary()) -> binary().
clean_query(QueryBin) when is_binary(QueryBin) ->
   Q0 = re:replace(QueryBin, "\n|\t|\r|;", " ",[global, {return, binary}]),
   Q = re:replace(Q0, "(\s){2,}", " ", [global, {return, binary}]),
   string:trim(Q).

%% check if the given string seems to be a valid "select ... from" or "with ... select ... from" statement
-spec check_select_statement(binary()) -> true|false.
check_select_statement(Q) ->
   Query = clean_query(Q),
   %% accepting "WITH" at the beginning
   Pattern = "^\s?((W|w)(I|i)(T|t)(H|h)\s+(.*))?(S|s)(E|e)(L|l)(E|e)(C|c)(T|t)\s+(.*)\s+(F|f)(R|r)(O|o)(M|m)\s+(.*)",
%%   Pattern = "^\s?(S|s)(E|e)(L|l)(E|e)(C|c)(T|t)\s+(.*)\s+(F|f)(R|r)(O|o)(M|m)\s+(.*)",
   case re:run(Query, Pattern) of
      nomatch -> false;
      _ -> true
   end.

%% convert a fun() to a readable string
-spec stringize_lambda(function()|#faxe_lambda{}) -> list().
stringize_lambda(#faxe_lambda{string = String}) ->
   String;
stringize_lambda(Fun) when is_function(Fun) ->
   Abs =
   case erlang:fun_info(Fun, env) of
      {env, [{_, _, _, Abs0}]} -> Abs0;
      {env, [{_, _, _, _, _, Abs1}]} -> Abs1
   end,
   Str = erl_pp:expr({'fun', 1, {clauses, Abs}}),
   io_lib:format("~s~p",[lists:flatten(Str)|"\n"]).

%% try to get byte_size of term
bytes(Data) ->
   erlang_term:byte_size(Data).
%%   case is_binary(Data) of
%%      true -> byte_size(Data);
%%      false ->
%%         case is_list(Data) of
%%            true ->
%%               case catch iolist_size(Data) of
%%                  Size when is_integer(Size) -> Size;
%%                  _ -> 0
%%               end;
%%            false -> 0
%%         end
%%   end.

%% @doc convert words to bytes with respect to the system's wordsize
bytes_from_words(Words) ->
   try
      Words * erlang:system_info(wordsize)
   catch
      _:_ -> 0
   end.

-spec local_ip_v4() -> tuple().
local_ip_v4() ->
   {ok, Addrs} = inet:getifaddrs(),
   A =
      [
         Addr || {_, Opts} <- Addrs, {addr, Addr} <- Opts,
         size(Addr) == 4, Addr =/= {127,0,0,1}
      ],
   case A of
      [] -> {127,0,0,1};
      [Ip|_] -> Ip
   end.

-spec ip_to_bin(tuple()) -> binary().
ip_to_bin({_A, _B, _C, _D} = Ip) ->
   list_to_binary(inet:ntoa(Ip)).

%% @doc try to get a unique name for the device we are running on
-spec device_name() -> binary().
device_name() ->
   try
      persistent_term:get({?MODULE, device_name})
   catch
      _:badarg ->
         Name = get_device_name(),
         persistent_term:put({?MODULE, device_name}, Name),
         Name
   end.
-spec get_device_name() -> binary().
get_device_name() ->
   %% first attempt is to try to get BALENA_DEVICE_UUID, should be set if we are running on balena
   %% RESIN_DEVICE_NAME_AT_INIT = balena device name
   case os:getenv(?KEY_BALENA_DEVICE_UUID) of
      false ->
         case os:getenv(?KEY_FAXE_DEVICE_UUID) of
            false ->
               %% so we are not running on balena and the FAXE env key is not set, then we use our local ip address
               Ip0 = ip_to_bin(local_ip_v4()),
               binary:replace(Ip0, <<".">>, <<"_">>, [global]);
            Id -> list_to_binary(Id)
         end;
      DeviceId ->
         list_to_binary(DeviceId)
   end.

-spec get_erlang_version() -> string().
get_erlang_version() ->
   case os:getenv("OTP_VERSION") of
      false -> erlang:system_info(otp_release) ++ "/" ++ erlang:system_info(version);
      Vs -> Vs
   end.

proplists_merge(L, T) ->
   lists:ukeymerge(1, lists:keysort(1,L), lists:keysort(1,T)).

-spec build_topic(list(list()|binary())) -> binary().
build_topic(Parts) when is_list(Parts) ->
   build_topic(Parts, <<"/">>).

-spec build_topic(list(list()|binary()), binary()) -> binary().
build_topic(Parts, Separator) when is_list(Parts) andalso is_binary(Separator) ->
   PartsBin = [to_bin(Part) || Part <- Parts],
   PartsClean = [
      estr:str_replace_leading(
         estr:str_replace_trailing(P, Separator, <<>>
         ), Separator, <<>>)
      || P <- PartsBin, P /= Separator],
   iolist_to_binary(lists:join(Separator, PartsClean)).


%% @doc get the first Depth parts of the given mqtt_topic
%% @param Topic an mqtt topic string (binary)
%% @param Depth starting from 1, how many parts of the given topic to return
%% @returns subtopic binary
subtopic(Topic, Depth) when is_binary(Topic), is_integer(Depth) ->
   TopicParts = lists:sublist(string:lexemes(Topic, "/"), Depth),
   build_topic(TopicParts).


-spec check_mqtt_topic(binary()) -> true | {false, binary()}.
check_mqtt_topic(<<>>) ->
   {false, <<"topic is empty">>};
check_mqtt_topic(<<"\"\"">>) ->
   {false, <<"topic is empty">>};
check_mqtt_topic(<<"/", _R/binary>>) ->
   {false, <<"topic must not start with '/'">>};
check_mqtt_topic(<<"$", _R/binary>>) ->
   {false, <<"topic must not start with '$'">>};
check_mqtt_topic(T) when is_binary(T) ->
   case estr:str_ends_with(T, <<"/">>) of
      true -> {false, <<"topic must not end with '/'">>};
      false -> true
   end.

-spec check_publisher_mqtt_topic(binary()) -> true | {false, binary()}.
check_publisher_mqtt_topic(T) when is_binary(T) ->
   case check_mqtt_topic(T) of
      true ->
         case binary:match(T, [<<"#">>, <<"+">>, <<"*">>]) of
            nomatch -> true;
            _ -> {false, <<"topic must not contain '#', '+', '*'">>}
         end;
      O -> O
   end.


-spec to_bin(any()) -> binary().
to_bin(L) when is_list(L) -> list_to_binary(L);
to_bin(E) when is_atom(E) -> atom_to_binary(E, utf8);
to_bin(Int) when is_integer(Int) -> integer_to_binary(Int);
to_bin(Float) when is_float(Float) -> float_to_binary(Float);
to_bin(Bin) -> Bin.

-spec to_list(L :: any()) -> list()|any().
to_list(L) when is_list(L) -> L;
to_list(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_list(Int) when is_integer(Int) -> integer_to_list(Int);
to_list(Float) when is_float(Float) -> float_to_list(Float);
to_list(Atom) when is_atom(Atom) -> atom_to_list(Atom);
to_list(E) -> E.

-spec to_num(any()) -> number()|term().
to_num(I) when is_binary(I) orelse is_list(I) ->
   case string_to_number(I) of
      false -> I;
      Num -> Num
   end;
to_num(I) -> I.

string_to_number(L) when is_list(L) ->
   string_to_number(list_to_binary(L));
string_to_number(L) when is_binary(L) ->
   Float = (catch erlang:binary_to_float(L)),
   case is_number(Float) of
      true -> Float;
      false -> Int = (catch erlang:binary_to_integer(L)),
         case is_number(Int) of
            true -> Int;
            false -> false
         end
   end.

save_binary_to_atom(Bin) when is_binary(Bin) ->
   case catch binary_to_existing_atom(Bin) of
      A when is_atom(A) -> A;
      _ -> throw("Cannot convert '" ++ binary_to_list(Bin) ++ "' to an atom!")
   end.

-spec to_rkey(binary()|list()) -> binary()|list().
to_rkey(Bin) when is_binary(Bin) ->
   binary:replace(
      binary:replace(
         Bin, <<"+">>, <<"*">>, [global]),
      <<"/">>,<<".">>,[global]
   );
to_rkey(List) when is_list(List) ->
   [to_rkey(E) || E <- List];
to_rkey(Other) ->
   Other.

%% erlang modulo
mod(X,Y) -> r_mod(X, Y).
r_mod(X,Y) when X > 0 -> X rem Y;
r_mod(X,Y) when X < 0 -> Y + X rem Y;
r_mod(0,_Y) -> 0.




%% Levenshtein code by Adam Lindberg, Fredrik Svensson via
%% http://www.trapexit.org/String_similar_to_(Levenshtein)
%%
%%------------------------------------------------------------------------------
%% @spec levenshtein(StringA :: string(), StringB :: string()) -> integer()
%% @doc Calculates the Levenshtein distance between two strings
%% @end
%%------------------------------------------------------------------------------
levenshtein(Samestring, Samestring) -> 0;
levenshtein(String, []) -> length(String);
levenshtein([], String) -> length(String);
levenshtein(Source, Target) ->
   levenshtein_rec(Source, Target, lists:seq(0, length(Target)), 1).

%% Recurses over every character in the source string and calculates a list of distances
levenshtein_rec([SrcHead|SrcTail], Target, DistList, Step) ->
   levenshtein_rec(SrcTail, Target, levenshtein_distlist(Target, DistList, SrcHead, [Step], Step), Step + 1);
levenshtein_rec([], _, DistList, _) ->
   lists:last(DistList).

%% Generates a distance list with distance values for every character in the target string
levenshtein_distlist([TargetHead|TargetTail], [DLH|DLT], SourceChar, NewDistList, LastDist) when length(DLT) > 0 ->
   Min = lists:min([LastDist + 1, hd(DLT) + 1, DLH + dif(TargetHead, SourceChar)]),
   levenshtein_distlist(TargetTail, DLT, SourceChar, NewDistList ++ [Min], Min);
levenshtein_distlist([], _, _, NewDistList, _) ->
   NewDistList.

% Calculates the difference between two characters or other values
dif(C, C) -> 0;
dif(_, _) -> 1.


%% @doc
%% Returns a random binary of size Length consisting of latins [a-zA-Z] and digits [0-9].
%% @end
-spec random_latin_binary(pos_integer()) -> binary().
random_latin_binary(Length) ->
   random_latin_binary(Length, any).

%% random_latin_binary/2
-spec random_latin_binary(Length :: pos_integer(), CaseFlag :: lower | upper | any) -> binary().
%% @doc
%% Returns a random binary of size Length consisting of latins [a-zA-Z] and digits [0-9].
%% The second argument CaseFlag corresponds to a letter case, an atom 'lower', 'upper' or 'any'.
%% @end
random_latin_binary(Length, CaseFlag) ->
   Chars = case CaseFlag of
              lower -> <<"abcdefghijklmnopqrstuvwxyz0123456789">>;
              upper -> <<"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789">>;
              any -> <<"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789">>
           end,
   random_binary_from_chars(Length, Chars).


%% random_binary_from_chars/2
-spec random_binary_from_chars(Length :: pos_integer(), Chars :: binary()) -> binary().
%% @doc
%% Generates and returns a binary of size Length which consists of the given characters Chars.
%% @end
random_binary_from_chars(Length, Chars) ->
   Bsize = erlang:byte_size(Chars),
   lists:foldl(
      fun(_, Acc) ->
         RndChar = binary:at(Chars, rand:uniform(Bsize)-1),
         << Acc/binary, RndChar >>
      end,
      <<>>,
      lists:seq(1, Length)
   ).

type(Bin) when is_binary(Bin) -> <<"string">>;
type(Int) when is_integer(Int) -> <<"integer">>;
type(Float) when is_float(Float) -> <<"float">>;
type(L) when is_list(L) -> <<"list">>;
type(T) when is_tuple(T) -> <<"tuple">>;
type(undefined) -> <<"undefined">>;
type(_) -> <<"unknown">>.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% TESTS %%%%%%%%%
-ifdef(TEST).

decimal_part_test() ->
   ?assertEqual(
      232,
      decimal_part(59.232, 3)
   ).
decimal_part_2_test() ->
   ?assertEqual(
      233,
      decimal_part(59.23266, 3)
   ).
decimal_part_3_test() ->
   ?assertEqual(
      23,
      decimal_part(59.23, 3)
   ).
decimal_part_4_test() ->
   ?assertEqual(
      23,
      decimal_part(59.2300, 3)
   ).
round_float_test() ->
   ?assertEqual(
      1234.232,
      round_float(1234.23214, 3)
   ).
round_float_2_test() ->
   ?assertEqual(
      2342.4567,
      round_float(2342.4567, 4)
   ).
round_float_3_test() ->
   ?assertEqual(
      3.456,
      round_float(3.456, 5)
   ).
prefix_binary_test() ->
   ?assertEqual(
      <<"myprefix">>,
      prefix_binary(<<"prefix">>, <<"my">>)
   ).

prefix_binary_2_test() ->
   ?assertEqual(
      <<"myprefix">>,
      prefix_binary(<<"myprefix">>, <<"m">>)
   ).
check_select_1_test() ->
   ?assertEqual(
      true,
      check_select_statement(<<"SELECT * FROM table">>)
   ).
check_select_2_test() ->
   ?assertEqual(
      true,
      check_select_statement(<<" SELECT\n COUNT(*), AVG(field) FROM docs.table WHERE foo = 'bar'\n
      GROUP BY time ORDER BY time">>)
   ).
check_select_3_test() ->
   ?assertEqual(
      false,
      check_select_statement(<<"INSERT INTO table SET foo = 'bar'">>)
   ).
check_select_4_test() ->
   Sql = <<"SELECT floor(EXTRACT(epoch FROM time)/300)*300 AS time_gb, COUNT(*) FROM table
      WHERE tag1 = 'test' AND time >= $1 AND time <= $2 GROUP BY time_gb, a, b ORDER BY time_gb DESC">>,
   ?assertEqual(
      true,
      check_select_statement(Sql)
   ).

check_select_with_test() ->
   Sql = <<"with \"task\" as (
    SELECT
      ts as \"tsTask\",
      {{ws_task_dbcol}}[''quantity''] as \"quantity\",
      {{ws_task_dbcol}}[''sourceSectionName''] as \"crateName\",
      {{ws_task_dbcol}}[''sourcelcaName''] AS \"lcaName\",
      {{ws_task_dbcol}}[''sku''] as \"sku\"
    FROM {{dest_schema}}.{{table}}
    where
      $__timefilter AND
      {{ws_task_dbcol}}[''quantity''] > 0 AND
      stream_id in ( {{ws_task_db_sid}} )
    )

  select
    \"task\".\"tsTask\" as \"ts\",
    {{crate_dbcol}}[''crateName''] as \"crateName\",
    {{crate_dbcol}}[''lcaName''] as \"lcaName\",
    array_max([{{crate_dbcol}}[''quantity''] - \"task\".\"quantity\", 0]) as \"quantity\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, true, false) as \"isEmpty\",
    {{crate_dbcol}}[''sku''] as \"sku\",
    if( {{crate_dbcol}}[''quantity''] - \"task\".\"quantity\" <= 0, ''Empty'', ''Broken'') as \"status\",
    ''None'' as \"reason\"
  FROM {{dest_schema}}.{{table}}, \"task\",
    (
    SELECT
      max(ts) as \"tsMax\",
      {{crate_dbcol}}[''lcaName''] as \"lcaName\"
    FROM {{dest_schema}}.{{table}}, \"task\"
    WHERE
      {{crate_dbcol}}[''lcaName''] = \"task\".\"lcaName\" and
      ts <= \"task\".\"tsTask\" and
      stream_id in ( {{crate_db_sid}}, {{crate_input_db_sid}} )
    GROUP By 2
    ) as \"tsMax\"
  where
    {{crate_dbcol}}[''lcaName''] = \"task\".\"lcaName\" and
    {{crate_dbcol}}[''lcaName''] = \"tsMax\".\"lcaName\" and
    ts = \"tsMax\".\"tsMax\" and
    stream_id in ( {{crate_db_sid}}, {{crate_input_db_sid}} )">>,
   ?assert(check_select_statement(Sql)).

ip_to_bin_test() ->
   Ip = {127, 0, 0, 1},
   ?assertEqual(<<"127.0.0.1">>, ip_to_bin(Ip)).

build_topic_1_test() ->
   Res = faxe_util:build_topic([<<"/ttgw/sys/faxe/">>, <<"/conn_status/hee/">>, <<"flowid/nodeid/#/">>], <<"/">>),
   Expected = <<"ttgw/sys/faxe/conn_status/hee/flowid/nodeid/#">>,
   ?assertEqual(Expected, Res).
build_topic_2_test() ->
   Expected = <<"ttgw/sys/faxe/conn_status/hee/flowid/nodeid/#">>,
   Res = faxe_util:build_topic(["/ttgw/sys/faxe/", <<"conn_status/hee">>, <<"flowid/nodeid/#/">>, "/"], <<"/">>),
   ?assertEqual(Expected, Res).
build_topic_3_test() ->
   Expected = <<"ttgw.sys.faxe.conn_status.hee.flowid.nodeid.#">>,
   Res = faxe_util:build_topic([<<".ttgw.sys.faxe.">>, "conn_status.hee.", <<"flowid.nodeid.#">>], <<".">>),
   ?assertEqual(Expected, Res).

map_flip_test() ->
   Map = #{k1 => v1, "k2" => "v2", <<"k3">> => <<"v3">>},
   Expected = #{v1 => k1, "v2" => "k2", <<"v3">> => <<"k3">>},
   ?assertEqual(Expected, faxe_util:flip_map(Map)).

map_flip_empty_test() ->
   ?assertEqual(#{}, faxe_util:flip_map(#{})).

to_rkey_1_test() ->
   Topic = <<"root/some/topic/here/#">>,
   Expected = <<"root.some.topic.here.#">>,
   ?assertEqual(Expected, to_rkey(Topic)).

to_rkey_2_test() ->
   Topic = <<"root/some/topic.dot/here/#">>,
   Expected = <<"root.some.topic.dot.here.#">>,
   ?assertEqual(Expected, to_rkey(Topic)).

to_rkey_3_test() ->
   Topic = <<"root/some/+/here/#">>,
   Expected = <<"root.some.*.here.#">>,
   ?assertEqual(Expected, to_rkey(Topic)).

to_rkey_4_test() ->
   Topic = <<"root/some/$__whatever/here-it-is/#">>,
   Expected = <<"root.some.$__whatever.here-it-is.#">>,
   ?assertEqual(Expected, to_rkey(Topic)).

to_rkey_undefined_test() ->
   Topic = undefined,
   Expected = undefined,
   ?assertEqual(Expected, to_rkey(Topic)).

to_rkey_list_test() ->
   Topics = [<<"root/some/ttopic/here/v2">>, <<"root/some/+/here/+/v1/#">>, <<"root/some/+/here/#">>],
   Expected = [<<"root.some.ttopic.here.v2">>, <<"root.some.*.here.*.v1.#">>, <<"root.some.*.here.#">>],
   ?assertEqual(Expected, to_rkey(Topics)).


check_topic_start_test() ->
   T = <<"/root/some/ttopic/here/v2">>,
   Expected = {false, <<"topic must not start with '/'">>},
   ?assertEqual(Expected, check_mqtt_topic(T)).

check_topic_end_test() ->
   T = <<"root/some/ttopic/here/v2/">>,
   Expected = {false, <<"topic must not end with '/'">>},
   ?assertEqual(Expected, check_mqtt_topic(T)).

check_topic_ok_test() ->
   T = <<"root/some/a/+/sdf/2342/ttopic/here/#">>,
   Expected = true,
   ?assertEqual(Expected, check_mqtt_topic(T)).

check_topic_start_dollar_test() ->
   T = <<"$SYS/root/some/ttopic/here/v2">>,
   Expected = {false, <<"topic must not start with '$'">>},
   ?assertEqual(Expected, check_mqtt_topic(T)).

check_topic_pub_start_test() ->
   T = <<"$SYS/root/some/ttopic/here/v2">>,
   Expected = {false, <<"topic must not start with '$'">>},
   ?assertEqual(Expected, check_publisher_mqtt_topic(T)).

check_topic_pub_wildcard1_test() ->
   T = <<"root/some/ttopic/here/v2/#">>,
   Expected = {false, <<"topic must not contain '#' or '+'">>},
   ?assertEqual(Expected, check_publisher_mqtt_topic(T)).

check_topic_pub_wildcard2_test() ->
   T = <<"root/some/ttopic/+/v2">>,
   Expected = {false, <<"topic must not contain '#' or '+'">>},
   ?assertEqual(Expected, check_publisher_mqtt_topic(T)).

check_topic_pub_ok_test() ->
   T = <<"root/some/ttopic/troll/lol/v2">>,
   Expected = true,
   ?assertEqual(Expected, check_publisher_mqtt_topic(T)).


-endif.

