defmodule ExMoQ.Relay do
  @moduledoc """
  Runs a [moq-relay](https://doc.moq.dev/bin/relay) binary as a supervised OS
  process.

  The struct holds the options of a relay to start (see `t:t/0`):

      {:ok, relay} = ExMoQ.Relay.start_link(%ExMoQ.Relay{quic: 4443})
      ExMoQ.Relay.info(relay).quic_url  #=> "https://127.0.0.1:4443"

  The relay stops with the process `start_link/1` returns, and that process
  exits with `{:exit_status, status}` when the relay exits on its own. Under
  a supervisor:

      {ExMoQ.Relay, %ExMoQ.Relay{quic: 4443, web: 4443, name: MyApp.Relay}}
  """

  use GenServer

  alias ExMoQ.Relay.{Args, Info, Options, Startup}

  @typedoc false
  @type patterns :: String.t() | [String.t(), ...]

  @typedoc false
  @type listener_opts :: [{:ip, :inet.ip_address()}]

  @typedoc false
  @type quic_opts :: [{:tls_generate, String.t() | nil} | {:ip, :inet.ip_address()}]

  @typedoc false
  @type listener :: :inet.port_number() | nil | {:inet.port_number(), listener_opts()}

  @typedoc "Path to the relay; when `nil` (default), see `find_binary/1`."
  @type relay_binary :: Path.t() | nil

  @typedoc """
  Address the listeners bind to (default `{127, 0, 0, 1}`). A listener binds
  to one of its own as `{port, ip: ip}`, like
  `web: {8080, ip: {127, 0, 0, 1}}` next to a public `:quic`.
  """
  @type ip :: :inet.ip_address()

  @typedoc """
  The QUIC listener: a port (default `4443`), `{port, opts}`, or `nil`. Its
  certificate is generated for `"localhost"`; `{port, tls_generate: host}`
  names another host, and `{port, tls_generate: nil}` generates none, for one
  passed in `:args`.
  """
  @type quic :: :inet.port_number() | nil | {:inet.port_number(), quic_opts()}

  @typedoc "A plaintext TCP listener: a port, `{port, opts}`, or `nil` (default)."
  @type tcp :: listener()

  @typedoc """
  The HTTP listener serving `moq-relay` [endpoints](https://doc.moq.dev/bin/relay/http.html):
  a port, `{port, opts}`, or `nil` (default).
  It may share its port number with `:quic`.
  """
  @type web :: listener()

  @typedoc "The HTTP listener for operators: a port, `{port, opts}`, or `nil` (default)."
  @type internal :: listener()

  @typedoc """
  How sessions are admitted (default `"**"`, everything): path patterns (a
  string or a list) for anonymous access; `[subscribe: patterns, publish:
  patterns]` to grant the two apart (`[subscribe: "**"]` lets anyone
  subscribe and no one publish); `{:url, url}` for an auth server the relay
  POSTs to (`http://`, `https://`, or `unix://`, see
  [Authentication](https://doc.moq.dev/bin/relay/auth)); or `nil` for none,
  leaving auth to `:args`.
  """
  @type auth ::
          patterns()
          | [{:subscribe, patterns()} | {:publish, patterns()}, ...]
          | {:url, String.t()}
          | nil

  @typedoc """
  The relay's log level (default `:warning`).
  """
  @type log_level :: Logger.level()

  @typedoc "Extra command-line arguments (default `[]`)."
  @type args :: [String.t()]

  @typedoc """
  Where the relay's output lines go: a `Logger` level to log them at (default
  `:info`), a 1-arity function to call with each, or `nil` to drop them.
  """
  @type output :: Logger.level() | (String.t() -> any()) | nil

  @typedoc "Milliseconds to wait for readiness (default `15_000`)."
  @type ready_timeout :: non_neg_integer()

  @typedoc "A name to register the relay's process under (default `nil`)."
  @type name :: GenServer.name() | nil

  @typedoc "Options of a relay to start."
  @type t :: %__MODULE__{
          binary: relay_binary(),
          ip: ip(),
          quic: quic(),
          tcp: tcp(),
          web: web(),
          internal: internal(),
          auth: auth(),
          log_level: log_level(),
          args: args(),
          output: output(),
          ready_timeout: ready_timeout(),
          name: name()
        }

  defstruct binary: nil,
            ip: {127, 0, 0, 1},
            quic: 4443,
            tcp: nil,
            web: nil,
            internal: nil,
            auth: "**",
            log_level: :warning,
            args: [],
            output: :info,
            ready_timeout: 15_000,
            name: nil

  @typedoc "Why the relay did not start."
  @type start_error ::
          {:validation_failed, String.t()}
          | {:binary_not_found, name :: Path.t()}
          | {:exit_status, non_neg_integer()}
          | {:not_ready, timeout_ms :: non_neg_integer()}

  @doc "A child spec for the relay; several may share a supervisor."
  @spec child_spec(t()) :: Supervisor.child_spec()
  def child_spec(%__MODULE__{} = relay) do
    %{
      id: {__MODULE__, relay.name || make_ref()},
      start: {__MODULE__, :start_link, [relay]},
      type: :worker
    }
  end

  @doc """
  Starts a relay linked to the caller and blocks until it is ready.
  """
  @spec start_link(t()) :: {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start_link(%__MODULE__{} = relay) do
    with {:ok, {_binary, options} = validated} <- validate(relay) do
      GenServer.start_link(__MODULE__, validated, name: options.name)
    end
  end

  @doc "Starts a relay not linked to the caller; see `start_link/1`."
  @spec start(t()) :: {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start(%__MODULE__{} = relay) do
    with {:ok, {_binary, options} = validated} <- validate(relay) do
      GenServer.start(__MODULE__, validated, name: options.name)
    end
  end

  @spec validate(t()) ::
          {:ok, {Path.t(), Options.t()}}
          | {:error, {:validation_failed, String.t()} | {:binary_not_found, Path.t()}}
  defp validate(%__MODULE__{} = relay) do
    with {:ok, options} <- Options.validate(relay),
         {:ok, binary} <- resolve_binary(relay.binary) do
      {:ok, {binary, options}}
    else
      {:error, message} when is_binary(message) -> {:error, {:validation_failed, message}}
      {:error, _reason} = error -> error
    end
  end

  @spec resolve_binary(relay_binary()) ::
          {:ok, Path.t()} | {:error, {:binary_not_found, Path.t()}}
  defp resolve_binary(binary) do
    name = binary_name(binary)

    case System.find_executable(name) do
      nil -> {:error, {:binary_not_found, name}}
      path -> {:ok, path}
    end
  end

  @doc "What the relay reports: where its listeners are reached."
  @spec info(GenServer.server()) :: Info.t()
  def info(relay), do: GenServer.call(relay, :info)

  @doc "Stops the relay."
  @spec stop(GenServer.server(), timeout()) :: :ok
  def stop(relay, timeout \\ 5_000), do: GenServer.stop(relay, :normal, timeout)

  @doc """
  Finds the relay binary: `binary` if given, else `$MOQ_RELAY`, else
  `moq-relay`. A bare name is looked up on `$PATH`; a path is expanded
  relative to the current directory. Returns `nil` if not found.
  """
  @spec find_binary(Path.t() | nil) :: Path.t() | nil
  def find_binary(binary \\ nil), do: System.find_executable(binary_name(binary))

  @spec binary_name(relay_binary()) :: Path.t()
  defp binary_name(binary) do
    name = binary || System.get_env("MOQ_RELAY") || "moq-relay"
    if String.contains?(name, "/"), do: Path.expand(name), else: name
  end

  @doc """
  The arguments the relay binary is run with.
  Raises `ArgumentError` when options fail validation.
  """
  @spec args(t()) :: [String.t()]
  def args(%__MODULE__{} = relay), do: relay |> Options.validate!() |> Args.args()

  ## Server

  @impl true
  def init({binary, %Options{} = options}) do
    case Startup.run(binary, options) do
      {:ok, %{daemon: daemon, monitor: monitor}} ->
        {:ok, %{daemon: daemon, monitor: monitor, info: Info.new(options)}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:info, _from, state), do: {:reply, state.info, state}

  # Need to map exit_status = 0 explicitly
  @impl true
  def handle_info({:DOWN, monitor, :process, _daemon, :normal}, %{monitor: monitor} = state),
    do: {:stop, {:exit_status, 0}, %{state | daemon: nil}}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{daemon: nil}), do: :ok

  def terminate(_reason, %{daemon: daemon}) do
    GenServer.stop(daemon)
  catch
    :exit, _reason -> :ok
  end
end
