defmodule ExMoQ.Relay.Info do
  @moduledoc """
  What a started relay reports, from `ExMoQ.Relay.info/1`: where its
  listeners are reached. See [`%ExMoQ.Relay.Info{}`](`__struct__/0`).
  """

  alias ExMoQ.Relay.Args
  alias ExMoQ.Relay.Options

  @doc """
  What a started relay reports.

  It contains these fields, one per listener of `ExMoQ.Relay`, each `nil`
  when that listener is off:

    * `:quic` - `{url, tls}`: the `https://host:port` of the QUIC listener
      (append the path yourself), and its certificate, `:generated` if
      self-signed by the relay or `:provided` if passed in `:args`.
    * `:tcp` - the `tcp://host:port` of the TCP listener.
    * `:web` - the `http://host:port` of the web listener.
    * `:internal` - the `http://host:port` of the internal listener.

  A listener bound to an unspecified address (`0.0.0.0` or `::`) is reached
  on the loopback one.
  """
  @enforce_keys [:quic, :tcp, :web, :internal]
  defstruct @enforce_keys

  @typedoc """
  What a started relay reports.

  See [`%ExMoQ.Relay.Info{}`](`__struct__/0`) for information about each field.
  """
  @type t :: %__MODULE__{
          quic: {String.t(), :generated | :provided} | nil,
          tcp: String.t() | nil,
          web: String.t() | nil,
          internal: String.t() | nil
        }

  @doc false
  @spec new(Options.t()) :: t()
  def new(%Options{} = options) do
    %__MODULE__{
      quic: quic(options),
      tcp: url(options, "tcp", :tcp),
      web: url(options, "http", :web),
      internal: url(options, "http", :internal)
    }
  end

  @spec quic(Options.t()) :: {String.t(), :generated | :provided} | nil
  defp quic(%Options{quic: nil}), do: nil

  defp quic(%Options{quic: {_port, opts}} = options) do
    tls = if opts[:tls_generate], do: :generated, else: :provided
    {url(options, "https", :quic), tls}
  end

  @spec url(Options.t(), String.t(), Options.endpoint()) :: String.t() | nil
  defp url(options, scheme, key) do
    case Args.listener(options, key) do
      nil -> nil
      {port, ip} -> "#{scheme}://#{Args.address(reachable(ip), port)}"
    end
  end

  @spec reachable(:inet.ip_address()) :: :inet.ip_address()
  defp reachable({0, 0, 0, 0}), do: {127, 0, 0, 1}
  defp reachable({0, 0, 0, 0, 0, 0, 0, 0}), do: {0, 0, 0, 0, 0, 0, 0, 1}
  defp reachable(ip), do: ip
end
