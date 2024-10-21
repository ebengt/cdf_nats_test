defmodule NatsTestIex.CDRPullConsumer do
  use Jetstream.PullConsumer

  def start_link(config) do
    Jetstream.PullConsumer.start_link(__MODULE__, config, [])
  end

  @impl true
  def init(config) do
    IO.puts("CDRPullConsumer init #{Kernel.inspect(config)}")
    state = state(config)
    {:ok, state, connection_name: :gnat, stream_name: "CDR", consumer_name: "CDR"}
  end

  @impl true
  def handle_message(message, state) do
    # :timer.sleep(1000)
    _body = message.body |> :erlang.binary_to_term() |> Kernel.inspect()
    IO.puts("CDRPullConsumer handle_message #{inspect(message)} #{Kernel.inspect(state)}")
    arrived(message, state)
    {state.reply, new_state(state)}
  end

  #
  # Internal functions
  #

  defp arrived(message, %{reply: :ack}), do: NatsTestIex.CDR.arrived(message.body)
  defp arrived(message, %{reply: :noreply}), do: NatsTestIex.CDR.arrived(message)

  defp new_state(state) do
    f = fn n -> n + 1 end
    Map.update!(state, :nr, f)
  end

  defp state(config), do: Map.put(config, :nr, 0) |> state_reply()
  defp state_reply(%{reply: _} = config), do: config
  defp state_reply(config), do: Map.put(config, :reply, :ack)
end
