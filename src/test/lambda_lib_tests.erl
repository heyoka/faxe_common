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
  Mapping = <<"[
  {\"key\": \"cond_vac\", \"dataformat\": \"0001.1110\", \"stream\":
  \"23u23jhr2iohjrfo23\", \"topic\" : \"data/0x867/23u23jhr2iohjrfo23/0001.1110\"},
  {\"key\": \"cond_scale\", \"dataformat\": \"0001.1120\", \"stream\": \"23898hduh2diuihd\", \"topic\" :
  \"data/0x867/23898hduh2diuihd/0001.1120\"},
  {\"key\": \"cond_robot\", \"dataformat\": \"0001.1130\",
   \"stream\": \"le232343242342343\", \"topic\" : \"le232343242342343/0001.1130\"}\n]">>,
  jiffy:decode(Mapping, [return_maps]).

mem_select_jarray_1_test() ->
  TheList = json_list_data(),
  Expected = <<"le232343242342343">>,
  ?assertEqual(Expected, faxe_lambda_lib:mem_select(<<"stream">>, [{<<"dataformat">>, <<"0001.1130">>}], TheList)).

mem_select_jarray_2_test() ->
  TheList = json_list_data(),
  Expected = <<"0001.1110">>,
  ?assertEqual(Expected, faxe_lambda_lib:mem_select(<<"dataformat">>, [{<<"key">>, <<"cond_vac">>}], TheList)).

mem_select_jarray_notfound_test() ->
  TheList = json_list_data(),
  Expected = [],
  Res = faxe_lambda_lib:mem_select(<<"key">>, [{<<"cond_drive">>, <<"dataformat">>}], TheList),
  ?assertEqual(Expected, Res).

mem_select_jarray_all_test() ->
  TheList = json_list_data(),
  Expected = [<<"cond_vac">>,<<"cond_scale">>,<<"cond_robot">>],
  Res = faxe_lambda_lib:mem_select_all(<<"key">>, TheList),
  ?assertEqual(Expected, Res).

mem_select_jarray_all_where_test() ->
  TheList = json_list_data(),
  Expected = <<"cond_vac">>,
  Res = faxe_lambda_lib:mem_select(<<"key">>, [{<<"dataformat">>, <<"0001.1110">>}], TheList),
  ?assertEqual(Expected, Res).

mem_select_key_undefined_test() ->
  TheList = json_list_data(),
  Expected = undefined,
  Res = faxe_lambda_lib:mem_select(<<"notkey">>, [{<<"dataformat">>, <<"0001.1110">>}], TheList),
  ?assertEqual(Expected, Res).

-endif.
