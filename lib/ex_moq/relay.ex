defmodule ExMoQ.Relay do
  @moduledoc """
  Runs a [moq-relay](https://doc.moq.dev/bin/relay) binary as a supervised OS
  process.

  The struct holds the options of a relay to start (see `t:t/0`):

      {:ok, relay} = ExMoQ.Relay.start_link(%ExMoQ.Relay{tcp: 4444})
      ExMoQ.Relay.info(relay).tcp_url  #=> "tcp://127.0.0.1:4444"

  The process returned by `start_link/1` owns the relay: the relay stops with
  it, and it exits with `{:exit_status, status}` when the relay exits with a
  non-zero status. Under a supervisor, the `:restart` setting decides what
  happens then:

      {ExMoQ.Relay, %ExMoQ.Relay{quic: 4443, web: 4443, name: MyApp.Relay}}

  Starting returns once the relay's listeners are bound and it accepts
  sessions. Where they are reached is in `info/1`.
  """

  use GenServer

  alias ExMoQ.Relay.{Args, Info, Options, Startup}

  @typedoc false
  @type patterns :: String.t() | [String.t()]

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
  The HTTP listener serving `/health`, `/certificate.sha256`, `/fetch` and
  WebSocket: a port, `{port, opts}`, or `nil` (default). It may share its port
  number with `:quic`.
  """
  @type web :: listener()

  @typedoc """
  The HTTP listener serving `/health`, `/metrics` and `/sessions`: a port,
  `{port, opts}`, or `nil` (default).
  """
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
          | [subscribe: patterns(), publish: patterns()]
          | {:url, String.t()}
          | nil

  @typedoc """
  The relay's log level (default `:warn`).
  """
  @type log_level :: :error | :warn | :info | :debug | :trace

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
            log_level: :warn,
            args: [],
            output: :info,
            ready_timeout: 15_000,
            name: nil

  @typedoc """
  Why the relay did not start: options failed validation, it exited with the
  given status, or it did not report readiness within the given milliseconds.
  """
  @type start_error ::
          {:validation_failed, String.t()}
          | {:exit_status, non_neg_integer()}
          | {:not_ready, timeout_ms :: non_neg_integer()}

  @doc """
  A child spec for the relay. Raises `ArgumentError` when options fail validation.
  """
  @spec child_spec(t()) :: Supervisor.child_spec()
  def child_spec(%__MODULE__{} = relay) do
    options = Options.validate!(relay)

    %{
      id: {__MODULE__, options.name || make_ref()},
      start: {__MODULE__, :start_link, [options]},
      type: :worker
    }
  end

  @doc """
  Starts a relay linked to the caller and blocks until it is ready.
  """
  @spec start_link(t()) :: {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start_link(%__MODULE__{} = relay) do
    case Options.validate(relay) do
      {:ok, options} -> start_link(options)
      {:error, reason} -> {:error, {:validation_failed, reason}}
    end
  end

  @doc false
  @spec start_link(Options.t()) ::
          {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start_link(%Options{} = options),
    do: GenServer.start_link(__MODULE__, options, name: options.name)

  @doc "Starts a relay not linked to the caller; see `start_link/1`."
  @spec start(t()) :: {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start(%__MODULE__{} = relay) do
    case Options.validate(relay) do
      {:ok, options} -> start(options)
      {:error, reason} -> {:error, {:validation_failed, reason}}
    end
  end

  @doc false
  @spec start(Options.t()) :: {:ok, pid()} | {:error, start_error() | {:already_started, pid()}}
  def start(%Options{} = options),
    do: GenServer.start(__MODULE__, options, name: options.name)

  @doc "What the relay reports: where its listeners are reached."
  @spec info(GenServer.server()) :: Info.t()
  def info(relay), do: GenServer.call(relay, :info)

  @doc "Stops the relay."
  @spec stop(GenServer.server(), timeout()) :: :ok
  def stop(relay, timeout \\ 5_000), do: GenServer.stop(relay, :normal, timeout)

  @doc """
  Finds the relay binary: `binary` if given, else `$MOQ_RELAY`, else
  `moq-relay` on `$PATH`. Returns `nil` when that is not an executable.
  """
  @spec find_binary(Path.t() | nil) :: Path.t() | nil
  def find_binary(binary \\ nil) do
    System.find_executable(binary || System.get_env("MOQ_RELAY") || "moq-relay")
  end

  @doc """
  The relay binary's version, e.g. `"0.15.8"`.
  """
  @spec version(Path.t() | nil) ::
          {:ok, String.t()}
          | {:error,
             :no_binary
             | {:exit_status, integer(), output :: String.t()}
             | {:unexpected_output, term()}}
  def version(binary \\ nil) do
    with path when is_binary(path) <- find_binary(binary) || {:error, :no_binary},
         {out, 0} <- System.cmd(path, ["--version"], stderr_to_stdout: true),
         ["moq-relay", version] <- out |> String.trim() |> String.split(" ", parts: 2) do
      {:ok, version}
    else
      {:error, _reason} = error -> error
      {out, status} when is_integer(status) -> {:error, {:exit_status, status, out}}
      other -> {:error, {:unexpected_output, other}}
    end
  end

  @doc """
  The arguments the relay binary is run with. Raises `ArgumentError` when
  options fail validation.
  """
  @spec args(t()) :: [String.t()]
  def args(%__MODULE__{} = relay), do: relay |> Options.validate!() |> Args.args()

  ## Server

  @impl true
  def init(%Options{} = options) do
    case Startup.run(options) do
      {:ok, %{daemon: daemon, monitor: monitor}} ->
        {:ok, %{daemon: daemon, monitor: monitor, info: Info.new(options)}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_call(:info, _from, state), do: {:reply, state.info, state}

  @impl true
  def handle_info({:DOWN, monitor, :process, _daemon, reason}, %{monitor: monitor} = state),
    do: {:stop, reason, %{state | daemon: nil}}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def terminate(_reason, %{daemon: nil}), do: :ok

  def terminate(_reason, %{daemon: daemon}) do
    GenServer.stop(daemon)
  catch
    :exit, _reason -> :ok
  end
end
