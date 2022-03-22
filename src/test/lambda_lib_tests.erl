%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. Mar 2022 16:01
%%%-------------------------------------------------------------------
-module(lambda_lib_tests).
-author("heyoka").

%% API
-include("faxe_common.hrl").

-ifdef(TEST).
-compile(nowarn_export_all).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").


json_list_data() ->
  Mapping = <<"[\n{\"key\": \"cond_vac\", \"dataformat\": \"0001.1110\", \"stream\":
  \"23u23jhr2iohjrfo23\", \"topic\" : \"data/0x867/23u23jhr2iohjrfo23/0001.1110\"},
  {\"key\": \"cond_scale\", \"dataformat\": \"0001.1120\", \"stream\": \"23898hduh2diuihd\", \"topic\" :
  \"data/0x867/23898hduh2diuihd/0001.1120\"},\n{\"key\": \"cond_robot\", \"dataformat\": \"0001.1130\",
   \"stream\": \"le232343242342343\", \"topic\" : \"data/0x867/le232343242342343/0001.1130\"}\n]">>,
  jiffy:decode(Mapping, [return_maps]).

select_listmaps_1_test() ->
  TheList = json_list_data(),
  Expected = <<"le232343242342343">>,
  ?assertEqual(Expected, faxe_lambda_lib:select(TheList, <<"dataformat">>, <<"0001.1130">>, <<"stream">>)).

select_listmaps_2_test() ->
  TheList = json_list_data(),
  Expected = <<"0001.1110">>,
  ?assertEqual(Expected, faxe_lambda_lib:select(TheList, <<"key">>, <<"cond_vac">>, <<"dataformat">>)).

select_listmaps_undefined_test() ->
  TheList = json_list_data(),
  Expected = undefined,
  Res = faxe_lambda_lib:select(TheList, <<"key">>, <<"cond_drive">>, <<"dataformat">>),
  ?assertEqual(Expected, Res).

-endif.
