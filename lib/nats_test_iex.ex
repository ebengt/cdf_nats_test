defmodule NatsTestIex do
  @moduledoc """
  Documentation for `NatsTestIex`.
  """

  # import Supervisor.Spec

  use Application

  alias Jetstream.API.{Consumer, Stream}

  @spec cdr_consumer_create(non_neg_integer()) :: {:ok, map()} | {:error, map()}
  def cdr_consumer_create(max_ack_pending, extra \\ %{}) do
    consumer =
      %{
        stream_name: "CDR",
        durable_name: "CDR",
        ack_wait: 50_000_000_000,
        max_deliver: 200,
        deliver_policy: :all,
        max_ack_pending: max_ack_pending
      }
      |> Map.merge(extra)

    Jetstream.API.Consumer.info(:gnat, consumer.stream_name, consumer.durable_name)
    |> cdr_consumer_correct(consumer)
  end

  # See http://elixir-lang.org/docs/stable/elixir/Application.html
  # for more information on OTP Applications
  def start(_type, _args) do
    hostname = (:os.getenv(~c"NATS_HOSTNAME") || ~c"localhost") |> to_string
    port = (:os.getenv(~c"NATS_PORT") || ~c"4222") |> to_string |> String.to_integer()
    pool_size = (:os.getenv(~c"NATS_POOL_SIZE") || ~c"10") |> to_string |> String.to_integer()

    children =
      [
        {Gnat.ConnectionSupervisor,
         %{
           name: :gnat,
           connection_settings: [
             %{host: hostname, port: port}
           ]
         }},
        NatsTestIex.QueueSupervisor,
        NatsTestIex.KVClientSupervisor,
        %{id: NatsTestIex.CDR, start: {NatsTestIex.CDR, :start_link, [[]]}}
      ] ++
        gnat_pool_children(hostname, port, pool_size) ++
        small_test_children(:os.getenv(~c"SMALL_TEST"))

    res = Supervisor.start_link(children, strategy: :one_for_one)
    :timer.sleep(100)
    small_test_create(:os.getenv(~c"SMALL_TEST"))
    # create_stream_consumer("HELLO", ["greetings.*.*"], 524_288_000, 200)
    # cdr_stream_consumer!()
    res
  end

  def gnat_pool_children(hostname, port, pool_size) do
    for i <- 1..pool_size do
      name = "gnat-#{i}" |> String.to_atom()
      supervisor_name = "gnat_connection_supervisor-#{i}" |> String.to_atom()

      settings = %{
        name: name,
        connection_settings: [
          %{host: hostname, port: port}
        ]
      }

      %{
        id: supervisor_name,
        start: {Gnat.ConnectionSupervisor, :start_link, [settings, [name: supervisor_name]]}
      }
    end
  end

  @doc """
  Setup NATS for our usecase.
  """
  def create_stream_consumer(name, subjects, max_bytes, max_deliver) do
    stream = %Stream{
      name: name,
      subjects: subjects,
      retention: :limits,
      discard: :old,
      max_bytes: max_bytes
    }

    {:ok, _response} = Stream.create(:gnat, stream)

    consumer = %Consumer{
      stream_name: name,
      durable_name: name,
      ack_wait: 5_000_000_000,
      max_deliver: max_deliver
    }

    {:ok, _response} = Consumer.create(:gnat, consumer)
  end

  @doc """
  Search for stream entries by "indexed" attributes

  ## Parameter `search_attrs`

  search_attrs is a map of index names and the value that we search for in the index.
  if an index is omitted a wildcard is used for its value.

  indexes are:
  * `:id` - uuid, should be unique among the subscribed topics
  * `:sequence` - integer, not unique
  """
  def search_in_archive(search_attrs) do
    id = Map.get(search_attrs, :id, "*")
    sequence = Map.get(search_attrs, :sequence, "*")

    Process.flag(:trap_exit, true)

    # setup consumer for searching the stream
    consumer = %Consumer{
      stream_name: "HELLO",
      durable_name: "SEARCHER",
      filter_subject: "greetings.#{id}.#{sequence}",
      ack_wait: 5_000_000_000
    }

    Consumer.create(:gnat, consumer)

    {:ok, consumer_pid} =
      Jetstream.PullConsumer.start_link(NatsTestIex.SearchingPullConsumer, [self(), id])

    # receive results
    receive do
      {:found_msg, msg} ->
        IO.puts(msg)
    after
      5000 ->
        IO.puts("nothing found after 5 seconds")
    end

    # gracefully tear down the consumer
    :ok = Jetstream.PullConsumer.close(consumer_pid)

    receive do
      {:EXIT, ^consumer_pid, :shutdown} ->
        nil
    end

    Consumer.delete(:gnat, "HELLO", "SEARCHER")
  end

  def small_test_name(),
    do: "small"

  defp cdr_consumer_correct({:ok, info}, config) do
    config_keys = Map.keys(config)
    f = fn value -> value end

    Map.take(info.config, config_keys)
    |> Map.update(:stream_name, config.stream_name, f)
    |> cdr_consumer_correct_config(config)
  end

  defp cdr_consumer_correct({:error, _error}, config) do
    IO.puts("CDR consumer create")
    consumer = Kernel.struct(Jetstream.API.Consumer, config)
    Jetstream.API.Consumer.create(:gnat, consumer)
  end

  defp cdr_consumer_correct_config(config, config), do: {:ok, %{config: config}}

  defp cdr_consumer_correct_config(old, config) do
    IO.puts("CDR consumer delete old")
    IO.inspect(old)
    Jetstream.API.Consumer.delete(:gnat, old.stream_name, old.durable_name)
    IO.puts("CDR consumer create")
    IO.inspect(config)
    consumer = Kernel.struct(Jetstream.API.Consumer, config)
    Jetstream.API.Consumer.create(:gnat, consumer)
  end

  if false do
    defp cdr_stream_consumer!() do
      stream = %{
        name: "CDR",
        subjects: ["one_cdr"],
        retention: :limits,
        discard: :old,
        max_bytes: 524_288_000
      }

      :ok = Jetstream.API.Stream.info(:gnat, stream.name) |> cdr_stream_correct(stream)

      # max_ack_pending default is 20000
      {:ok, _} = cdr_consumer_create(20_000)
    end
  end

  if false do
    defp cdr_stream_correct({:ok, info}, config) do
      config_keys = Map.keys(config)
      Map.take(info.config, config_keys) |> cdr_stream_correct_config(config)
    end

    defp cdr_stream_correct({:error, _error}, config) do
      IO.puts("CDR stream create")
      stream = Kernel.struct(Jetstream.API.Stream, config)
      Jetstream.API.Stream.create(:gnat, stream) |> Kernel.elem(0)
    end

    defp cdr_stream_correct_config(config, config), do: :ok

    defp cdr_stream_correct_config(old, config) do
      IO.puts("CDR stream delete old")
      Jetstream.API.Stream.delete(:gnat, old.name)
      IO.puts("CDR stream create")
      stream = Kernel.struct(Jetstream.API.Stream, config)
      Jetstream.API.Stream.create(:gnat, stream) |> Kernel.elem(0)
    end
  end

  defp small_test_children(false), do: []

  defp small_test_children(_) do
    intermediate = small_test_children_intermediate()
    pull_consumer = small_test_children_pull_consumer()
    [intermediate, pull_consumer]
  end

  defp small_test_children_intermediate(),
    do: %{id: :intermediate, start: {:ercdf_nats_intermediate, :start_link, [%{}]}}

  defp small_test_children_pull_consumer() do
    config = %{
      connection_name: :gnat,
      stream_name: "small",
      consumer_name: "small",
      destination_module: :ercdf_nats_intermediate
    }

    %{id: :small_pull_consumer, start: {NatsTestIex.SmallPullConsumer, :start_link, [config]}}
  end

  defp small_test_create(false), do: :ok

  defp small_test_create(_),
    do: create_stream_consumer(small_test_name(), ["small.string"], 10_000_000, 200)
end
