%% Date: 06.04.17 - 22:48
%% Ⓒ 2017 heyoka
%%
%% @doc
%% A lambda in faxe is an erlang fun, which receives a #data_point record
%% and returns another fun, which is the actual lambda
%%
%% This module will evaluate both and return either the result of the inner-fun
%% or it will set #data_point's field or tag to the resulting value
%% @end
%%
-module(faxe_lambda).
-author("Alexander Minichmair").

-include("dataflow.hrl").

%% API
-export([execute/3, execute/2, execute_bool/2, ensure/1, is_lambda/1]).

is_lambda(#faxe_lambda{}) -> true;
is_lambda(_) -> false.

ensure(#faxe_lambda{function = Name0} = L) ->
   case code:is_loaded(Name0) of
      false ->
         compile_and_load(L);
      {file, _} -> ok
   end,
   L;
ensure([#faxe_lambda{}|_] = Ls) ->
   [ensure(L) || L <- Ls].

compile_and_load(#faxe_lambda{string = LambdaString, function = Name0}) ->
   Name = atom_to_list(Name0),
   ModuleString = "-module("++Name++"). \n -export(["++Name++"/1]). \n" ++ LambdaString ++ "\n",
   ets:insert(faxe_lambdas, {Name0, LambdaString}),
   {module, Name0} = dynamic_compile:load_from_string(ModuleString).


%% @doc
%% evaluates a faxe lambda and returns the result
%% @end
-spec execute(#data_point{}, #faxe_lambda{}) -> any().
execute(#data_point{} = Point, #faxe_lambda{module = Mod, function = Func}) ->
   Mod:Func(Point).

%% @doc
%% evaluates an erlang fun and returns the given data_point with the result of the evaluation
%% set to the field named As
%% @end
-spec execute(#data_point{}, #faxe_lambda{}, binary()) -> any().
execute(#data_point{} = Point, Lambda, As) ->
   LVal = execute(Point, Lambda),
   flowdata:set_field(Point, As, LVal).

%% @doc
%% evaluate lambda and check if return type is boolean
%% does not cast to a boolean value !
%% @end
-spec execute_bool(#data_point{}, #faxe_lambda{}) -> true | false | term().
execute_bool(Point, Lambda) ->
%%   case dfs_std_lib:bool(execute(Point, Lambda)) of
   case execute(Point, Lambda) of
      true -> true;
      false -> false;
      Else ->
         lager:warning("Lamdba ~p has no boolean return value on item: ~p (returned: ~p).",
         [faxe_util:stringize_lambda(Lambda), Point, Else]),
         Else
   end.
