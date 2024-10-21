defmodule NatsTestIex.TooManyCDRTest do
  use ExUnit.Case

  test "NATS intermediate started" do
    Gnat.pub(:gnat, "small.string", :erlang.term_to_binary("started"))
    result = :ercdf_nats_intermediate.read_new(1)
    assert Enum.count(result) === 1
  end

  test "Overfill NATS stream, read correct amount of items" do
    n = fill_stream(true, items_in_stream())
    result = read_all()
    assert Enum.count(result) === n
  end

  defp items_in_stream() do
    {:ok, info} = Gnat.Jetstream.API.Stream.info(:gnat, NatsTestIex.small_test_name())
    info.state.messages
  end

  defp fill_stream(true, n) do
    Gnat.pub(:gnat, "small.string", :erlang.term_to_binary("#{n} string"))
    (items_in_stream() === n) |> fill_stream(n + 1)
  end

  defp fill_stream(false, n), do: n - 1

  defp read_all(), do: :ercdf_nats_intermediate.read_new(100) |> read_all([])

  defp read_all([], acc), do: acc
  defp read_all(items, acc), do: :ercdf_nats_intermediate.read_new(100) |> read_all(items ++ acc)
end
