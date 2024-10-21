%% Nats messages that arrive from PullConsumer are saved here.

-module( ercdf_nats_intermediate ).

-behaviour( gen_server ).

%% Callbacks
-export( [init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3] ).

%% API
-export( [
	message_arrived/1,
	read_new/1,
	start_link/1
] ).
-ifdef(TEST). % Defined by 'rebar3 ct'
-export([
	message_arrived_foldr/1,
	message_arrived_tagged/1,
	message_arrived_tailor_made/1,
	message_reset/0
]).
-endif.

-include_lib( "kernel/include/logger.hrl" ).

-ignore_xref( [message_arrived/1] ). % Used from ercdf_ellxir_code repo.
-ignore_xref( [start_link/1] ).

-spec( message_arrived(map()) -> ok | {eror, Reason::any()} ).
message_arrived( Message ) -> gen_server:call( ?MODULE, {message_arrived_tailor_made, Message} ).

-spec( read_new(integer()) -> [CDR::map()] ).
read_new( Count ) -> gen_server:call( ?MODULE, {read_new, Count} ).

-spec( start_link(map()) -> {ok, pid()} | {eror, Reason::any()} ).
start_link( Config ) -> gen_server:start_link( {local, ?MODULE}, ?MODULE, Config, [] ).

-ifdef(TEST). % Defined by 'rebar3 ct'
message_arrived_foldr( Message ) -> gen_server:call( ?MODULE, {message_arrived_foldr, Message} ).
message_arrived_tagged( Message ) -> gen_server:call( ?MODULE, {message_arrived_tagged, Message} ).
message_arrived_tailor_made( Message ) -> gen_server:call( ?MODULE, {message_arrived_tailor_made, Message} ).
message_reset( ) -> gen_server:cast( ?MODULE, {message_reset} ).
-endif.

%% Callbacks
init( Config ) ->
	{ok, state( Config )}.

handle_call( {read_new, Count}, _From, #{acking := A, messages := M, count := C} = State ) when Count > C ->
	?LOG( info, "~p:~p: read_new ~p (~p)", [?MODULE, ?FUNCTION_NAME, Count, C] ),
	Acker = ack( M ),
	Result = lists:filtermap( cdr_safe_decode(State), M ),
	New_state = State#{acking => [Acker | A], messages => [], count => 0},
	{reply, Result, New_state};

handle_call( {read_new, Count}, _From, #{acking := A, messages := M, count := C} = State ) ->
	?LOG( info, "~p:~p: read_new ~p (~p)", [?MODULE, ?FUNCTION_NAME, Count, C] ),
	Remaining = C - Count,
	{Tail, First} = lists:split( Remaining, M ),
	Acker = ack( First ),
	Result = lists:filtermap( cdr_safe_decode(State), First ),
	New_state = State#{acking => [Acker | A], messages => Tail, count => Remaining},
	{reply, Result, New_state};
%% Test
handle_call( {message_arrived_foldr, Message}, _From, #{messages := M, count := C} = State ) ->
	{New_messages, New_count} = message_arrived( Message, M, C ),
	New_state = State#{messages => New_messages, count => New_count},
	{reply, ok, New_state};
%% Test
handle_call( {message_arrived_tagged, Message}, _From, #{messages := M, count := C} = State ) ->
	{New_messages, New_count} = message_arrived_tagged( Message, M, C ),
	New_state = State#{messages => New_messages, count => New_count},
	{reply, ok, New_state};

handle_call( {message_arrived_tailor_made, Message}, _From, #{messages := M, count := C} = State ) ->
	?LOG( info, "~p:~p: message_arrived ~p", [?MODULE, ?FUNCTION_NAME, Message] ),
	{New_messages, New_count} = message_arrived_tailor_made( Message, M, C ),
	New_state = State#{messages => New_messages, count => New_count},
	{reply, ok, New_state};

handle_call( Request, _From, State ) ->
	?LOG( error, "~p:~p: ~p", [?MODULE, ?FUNCTION_NAME, Request] ),
	{reply, ok, State}.

handle_cast( {message_reset}, State ) ->
	New_state = State#{messages => [], count => 0},
	{noreply, New_state};

handle_cast( Request, State ) ->
	?LOG( error, "~p:~p: ~p", [?MODULE, ?FUNCTION_NAME, Request] ),
	{noreply, State}.

handle_info( {ack, Acker}, #{acking := A} = State ) ->
	New_a = ack_log( lists:keytake(Acker, 1, A), A ),
	New_state = State#{acking => New_a},
	{noreply, New_state};

handle_info( Info, State ) ->
	?LOG( error, "~p:~p: ~p", [?MODULE, ?FUNCTION_NAME, Info] ),
	{noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_Old, State, _Extra) -> {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

ack( Messages ) ->
	Self = erlang:self(),
	{erlang:spawn( fun() -> ack(Messages, Self) end ), os:system_time(millisecond)}.

ack( Messages, Reply_to ) ->
	try
		['Elixir.Gnat.Jetstream':ack( X ) || X <- Messages]
	catch Error:Reason:Stack ->
		?LOG( error, "Jetstream ack failed at ~p\n.Reason ~p ~p", [Stack, Error, Reason] )
	after
		Reply_to ! {ack, erlang:self()}
	end.

ack_log(false, Acking ) ->
	?LOG( error, "~p Acker not found. Should not happen, but nothing to do.", [?MODULE] ),
	Acking;
ack_log({value, {_, Start}, Acking}, _Acking ) ->
	ack_log(os:system_time(millisecond) - Start ),
	Acking.

ack_log( Duration ) when Duration > 5000 ->
	?LOG( error, "~p Acking too slow ~p ms.", [?MODULE, Duration] );
ack_log( Duration ) when Duration > 1000 ->
	?LOG( warning, "~p Acking slow ~p ms.", [?MODULE, Duration] );
ack_log( Duration ) ->
	?LOG( info, "~p Acking ~p ms.", [?MODULE, Duration] ).


cdr_safe_decode( #{json := true} ) -> fun (Message) -> cdr_safe_decode( Message, fun ercdf_nats_json:decode/1 ) end;
cdr_safe_decode( _ ) -> fun (Message) -> cdr_safe_decode( Message, fun erlang:binary_to_term/1 ) end.

cdr_safe_decode( #{body := ""}, _Fun ) ->
	?LOG( error, "~p No encoded CDR.", [?MODULE] ),
	false;
cdr_safe_decode( #{body := Body}, Fun ) ->
	try
		{true, Fun(Body)}
	catch Error:Reason:Stack ->
		?LOG( error, "CDR decode failed at ~p\n.Reason ~p ~p.\nData ~p.", [Stack, Error, Reason, Body] ),
		false
	end.


%% Either update an existing message with the new reply_to. Keep order by lists:foldr/3.
%% Or add a new message.
message_arrived( Message, Messages, Count ) ->
	message_arrived_add_new( lists:foldr( fun message_arrived_foldr/2, {[], maps:get(body, Message), Message}, Messages), Messages, Count ).

%% New message to add. Increment count.
message_arrived_add_new( {_Messages, _Body, New}, Messages, Count ) -> {[New | Messages], Count + 1};
%% Messages updated with reply_to from re-sent message. No new message.
message_arrived_add_new( Updated_messages, _Old, Count ) -> {Updated_messages, Count}.

message_arrived_foldr( Message, {Acc, Body, New} ) ->
	message_arrived_foldr_exist( maps:get(body, Message) =:= Body, Message, Acc, Body, New );
%% New message have updated accumulator. Go through the rest of the messages.
message_arrived_foldr( Message, Acc ) -> [Message | Acc].

%% Replace an existing message with the new reply_to. Keep accumulating rest of messages.
message_arrived_foldr_exist( true, _Message, Acc, _Body, New ) -> [New | Acc];
%% Keep going.
message_arrived_foldr_exist( false, Message, Acc, Body, New ) -> {[Message | Acc], Body, New}.


%% This can only happen during test. NATS will not allow #{body := tuple()} in normal case.
message_arrived_tagged( #{body := {Tag, CDR}} = New, Messages, Count ) ->
	message_arrived_tagged_add_new( lists:foldr( fun message_arrived_tagged_foldr/2, {[], Tag, CDR, New}, Messages), Messages, Count );
message_arrived_tagged( #{body := B} = New, Messages, Count ) ->
	{Tag, CDR} = erlang:binary_to_term( B ),
	message_arrived_tagged_add_new( lists:foldr( fun message_arrived_tagged_foldr/2, {[], Tag, CDR, New}, Messages), Messages, Count ).

%% New message to add. Increment count.
message_arrived_tagged_add_new( {_Messages, Tag, CDR, New}, Messages, Count ) -> {[{Tag, CDR, New} | Messages], Count + 1};
%% Messages updated with reply_to from re-sent message. No new message.
message_arrived_tagged_add_new( Updated_messages, _Old, Count ) -> {Updated_messages, Count}.

%% Replace an existing message with the new reply_to. Continue accumulating rest of messages.
message_arrived_tagged_foldr( {Tag, _CDR, _Message}, {Acc, New_tag, New_CDR, New} ) when Tag =:= New_tag ->
	[{New_tag, New_CDR, New} | Acc];
%% Keep looking.
message_arrived_tagged_foldr( {Tag, CDR, Message}, {Acc, New_tag, New_CDR, New} ) ->
	{[{Tag, CDR, Message} | Acc], New_tag, New_CDR, New};
%% New message have updated accumulator. Accumulating the rest of the messages.
message_arrived_tagged_foldr( Message, Acc ) -> [Message | Acc].


message_arrived_tailor_made( New, Messages, Count ) ->
	message_arrived_tailor_made( Messages, New, maps:get(body, New), [], Messages, Count ).

message_arrived_tailor_made( [], New, _Body, _Acc, Messages, Count ) -> {[New | Messages], Count + 1};
message_arrived_tailor_made( [M | T], New, Body, Acc, Messages, Count ) ->
	message_arrived_tailor_made_exist( maps:get(body, M) =:= Body, M, T, New, Body, Acc, Messages, Count ).

message_arrived_tailor_made_exist( true, _Existing, T, New, _Body, Acc, _Messages, Count ) ->
	{lists:reverse([New | Acc]) ++ T, Count};
message_arrived_tailor_made_exist( false, Existing, T, New, Body, Acc, Messages, Count ) ->
	message_arrived_tailor_made( T, New, Body, [Existing | Acc], Messages, Count ).


state( #{json := J} ) -> #{acking => [], messages => [], count => 0, json => J};
state( _Config ) -> #{acking => [], messages => [], count => 0}.
