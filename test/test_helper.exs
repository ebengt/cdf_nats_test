ExUnit.start()

defmodule NatsTestIex.TestHelper do
  def cdr_empty() do
    cdr_await_process(nil)
    # Allow old items in stream to propagate
    Process.sleep(999)
    NatsTestIex.CDR.get() |> cdr_await_empty("")
  end

  def cdr_get_one(), do: NatsTestIex.CDR.get() |> cdr_get_one()
  def cdr_start(), do: cdr_start(%{})

  def cdr_start(config) do
    s = Kernel.self()
    pid = Process.spawn(fn -> cdr_start_wait(config, s) end, [])

    receive do
      {:started, ^pid} -> :ok
    end

    pid
  end

  def cdr_stop(pid, kind) do
    # Allow old items in stream to propagate
    Process.sleep(999)
    NatsTestIex.CDR.get() |> cdr_await_empty("stop")
    Process.exit(pid, kind)
  end

  #
  # Internal functions
  #

  defp cdr_await_empty([], _), do: :ok

  defp cdr_await_empty([old], label) do
    IO.puts("Unempty CDR #{label}: #{Kernel.inspect(old)}")
    NatsTestIex.CDR.get() |> cdr_await_empty(label)
  end

  defp cdr_await_process(nil), do: NatsTestIex.CDR |> Process.whereis() |> cdr_await_process()
  defp cdr_await_process(_), do: :ok

  defp cdr_get_one([]) do
    Process.sleep(999)
    NatsTestIex.CDR.get() |> cdr_get_one()
  end

  defp cdr_get_one([one]), do: one

  defp cdr_start_wait(config, reply_to) do
    _ = NatsTestIex.CDRPullConsumer.start_link(config)
    Process.send(reply_to, {:started, Kernel.self()}, [])
    Process.sleep(100_000)
  end
end
