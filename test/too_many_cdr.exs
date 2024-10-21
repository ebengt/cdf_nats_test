defmodule NatsTestIex.TooManyCDRTest do
  use ExUnit.Case

  test "too many CDR" do
    Gnat.pub(:gnat, "small.string", :erlang.term_to_binary("string"))
    Process.sleep(1000)
    result = :ercdf_nats_intermediate.read_new(1)
    assert Enum.count(result) === 1
  end
end
