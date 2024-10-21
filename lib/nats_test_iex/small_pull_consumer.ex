defmodule NatsTestIex.SmallPullConsumer do
  @moduledoc """
   Pull Consumer Code for stream (called small) with max bytes 1000
  """
  use Gnat.Jetstream.PullConsumer

  def start_link(config), do: Gnat.Jetstream.PullConsumer.start_link(__MODULE__, config, [])

  @impl true
  def init(config) do
    state = state(config)
    IO.puts("state #{Kernel.inspect(state)}")
    connection_options = connection_options(config)
    IO.puts("connection_options #{Kernel.inspect(connection_options)}")
    {:ok, state, connection_options}
  end

  @impl true
  def handle_message(message, state) do
    m = state.destination_module
    m.message_arrived(message)
    {:noreply, state}
  end

  #
  # Internal functions
  #

  defp connection_options(config),
    do:
      Map.take(config, [
        :connection_name,
        :stream_name,
        :consumer_name,
        :connection_retry_timeout,
        :connection_retries,
        :domain
      ])
      |> Enum.into(Keyword.new())

  defp state(config), do: Map.take(config, [:destination_module])
end
