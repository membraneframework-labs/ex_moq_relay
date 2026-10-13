defmodule ExMoQ.Testing.Relay do
  @moduledoc """
  A relay for ExUnit tests, reached over TCP and stopped with the test.

      %ExMoQ.Relay.Info{tcp_url: url} = ExMoQ.Testing.Relay.start_supervised!()

  To stop it before the test ends, start it with ExUnit directly:

      relay = start_supervised!({ExMoQ.Relay, ExMoQ.Testing.Relay.options()}, id: :relay)
      ExMoQ.Relay.info(relay).tcp_url
      stop_supervised!(:relay)
  """

  @doc """
  The options of a test relay: TCP only, on a port free as they are built
  (see `free_port/0`), with its output logged at `:debug`. `binary` is
  resolved with `find_binary/1`.
  """
  @spec options(Path.t() | nil) :: ExMoQ.Relay.t()
  def options(binary \\ nil) do
    %ExMoQ.Relay{binary: binary, tcp: free_port(), quic: nil, log_level: :info, output: :debug}
  end

  @doc """
  A TCP port nothing listens on right now, for a relay to bind. Something
  else may take it before the relay does, which is rare.
  """
  @spec free_port() :: :inet.port_number()
  def free_port() do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :ok = :gen_tcp.close(socket)
    port
  end

  @doc """
  Starts a relay with `options/1` under the test supervisor and blocks until
  it is ready.

  Raises if there is no binary, or the relay exits or is not ready in time.
  """
  @spec start_supervised!(Path.t() | nil) :: ExMoQ.Relay.Info.t()
  def start_supervised!(binary \\ nil) do
    {ExMoQ.Relay, options(binary)}
    |> ExUnit.Callbacks.start_supervised!(restart: :temporary)
    |> ExMoQ.Relay.info()
  end
end
