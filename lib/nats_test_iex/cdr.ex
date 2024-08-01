defmodule NatsTestIex.CDR do
  use GenServer

  def arrived(cdr), do: GenServer.cast(__MODULE__, {:push, cdr})
  def count(), do: GenServer.call(__MODULE__, :count)
  def get(), do: GenServer.call(__MODULE__, :pop)
  def start_link(stack), do: GenServer.start_link(__MODULE__, stack, name: __MODULE__)

  @impl true
  def init(stack) do
    IO.puts("NatsTestIex.CDR init #{Kernel.inspect(stack)}")
    {:ok, stack}
  end

  @impl true
  def handle_call(:count, _from, stack) do
    {:reply, Enum.count(stack), stack}
  end

  def handle_call(:pop, _from, []) do
    IO.puts("NatsTestIex.CDR pop []")
    {:reply, [], []}
  end

  def handle_call(:pop, _from, [head | tail]) do
    result = ack_next(head) |> :erlang.binary_to_term()
    IO.puts("NatsTestIex.CDR pop #{Kernel.inspect(result)} #{Enum.count(tail)}")
    {:reply, [result], tail}
  end

  @impl true
  def handle_cast({:push, element}, state) do
    result = body(element) |> :erlang.binary_to_term()
    IO.puts("NatsTestIex.CDR push #{Kernel.inspect(result)} #{Enum.count(state)}")
    {:noreply, [element | state]}
  end

  #
  # Internal functions
  #

  defp ack_next(%{body: body} = message) do
    Jetstream.ack_next(message, "cdr")
    body
  end

  defp ack_next(body), do: body

  defp body(%{body: body} = _message), do: body

  defp body(body), do: body
end
