defmodule ExMoQ.Relay.Startup do
  @moduledoc false
  # Starts the moq-relay OS process and waits until it is ready: NOTIFY_SOCKET
  # reports READY=1, and any `:auto` ports are resolved from listening log lines.
  # Called from `ExMoQ.Relay` init; the GenServer is the mailbox for notify and
  # scraped ports, and stays linked to the daemon.

  require Logger

  alias ExMoQ.Relay
  alias ExMoQ.Relay.{Args, Options}

  @type t :: %{
          daemon: pid(),
          monitor: reference(),
          options: Options.t()
        }

  @spec run(Options.t()) :: {:ok, t()} | {:error, term()}
  def run(%Options{} = options) do
    auto_ports? = not ports_resolved?(options)

    logs_above_level? =
      auto_ports? and Options.level_index(options.log_level) < Options.level_index(:info)

    deadline = System.monotonic_time(:millisecond) + options.ready_timeout
    path = notify_path()
    # Ensure no leftover from previous run
    File.rm(path)
    {:ok, notify} = :gen_udp.open(0, [:local, :binary, active: true, ifaddr: {:local, path}])

    daemon_opts = [
      stderr_to_stdout: true,
      exit_status_to_reason: &{:exit_status, &1},
      logger_fun: logger_fun(options, self(), auto_ports?, logs_above_level?),
      env: env(options, path, logs_above_level?)
    ]

    # The daemon's init fails only when the validated binary is gone, muontrap's
    # own binary is missing, or the VM is out of file descriptors. Being linked,
    # that failure exits this process with its reason rather than returning
    # `{:error, _}`, which is as it should be: none of those are handled.
    {:ok, daemon} = MuonTrap.Daemon.start_link(options.binary, Args.args(options), daemon_opts)
    monitor = Process.monitor(daemon)

    result =
      case await_ready(options, false, notify, monitor, deadline) do
        {:ok, options} ->
          {:ok, %{daemon: daemon, monitor: monitor, options: options}}

        {:error, {:not_ready, _timeout_ms} = reason} ->
          stop_daemon(daemon)
          {:error, reason}

        {:error, reason} ->
          {:error, reason}
      end

    :ok = :gen_udp.close(notify)
    File.rm(path)
    result
  end

  @spec notify_path() :: Path.t()
  defp notify_path() do
    name = "moq-relay-#{System.pid()}-#{System.unique_integer([:positive])}.sock"
    Path.join(System.tmp_dir!(), name)
  end

  @spec env(Options.t(), Path.t(), boolean()) :: [{String.t(), String.t()}]
  defp env(options, notify_path, logs_above_level?) do
    directives =
      if logs_above_level?,
        do: Enum.map(["moq_relay::relay", "moq_relay::web", "moq_tokio::server"], &"#{&1}=info"),
        else: []

    [
      {"NOTIFY_SOCKET", notify_path},
      {"NO_COLOR", "1"},
      {"RUST_LOG", Enum.join([Atom.to_string(options.log_level) | directives], ",")}
    ]
  end

  # Returns the options with `:auto` ports resolved once the relay is ready.
  @spec await_ready(Options.t(), boolean(), :gen_udp.socket(), reference(), integer()) ::
          {:ok, Options.t()} | {:error, term()}
  defp await_ready(options, notified?, notify, monitor, deadline) do
    if notified? and ports_resolved?(options) do
      {:ok, options}
    else
      timeout = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:udp, ^notify, _address, _port, message} ->
          ready? = "READY=1" in String.split(message, "\n")
          await_ready(options, notified? or ready?, notify, monitor, deadline)

        {:listening, listener, port} ->
          options = resolve(options, listener, port)
          await_ready(options, notified?, notify, monitor, deadline)

        {:DOWN, ^monitor, :process, _daemon, :normal} ->
          {:error, {:exit_status, 0}}

        {:DOWN, ^monitor, :process, _daemon, reason} ->
          {:error, reason}
      after
        timeout -> {:error, {:not_ready, options.ready_timeout}}
      end
    end
  end

  @spec ports_resolved?(Options.t()) :: boolean()
  defp ports_resolved?(options),
    do: not Enum.any?([:quic, :tcp, :web], &match?({:auto, _ip}, Args.listener(options, &1)))

  @spec resolve(Options.t(), :quic | :tcp | :web, :inet.port_number()) :: Options.t()
  defp resolve(options, listener, port) do
    Map.update!(options, listener, fn
      :auto -> port
      {:auto, opts} -> {port, opts}
      other -> other
    end)
  end

  # Runs in the daemon's process, so the lines a relay prints as it exits are
  # written out before the exit reaches the relay's process.
  @spec logger_fun(Options.t(), pid(), boolean(), boolean()) :: (String.t() -> :ok)
  defp logger_fun(options, server, auto_ports?, logs_above_level?) do
    fn line ->
      if not logs_above_level? or within_level?(line, options.log_level),
        do: output(options, line)

      with true <- auto_ports?,
           {listener, port} <- listening(line),
           do: send(server, {:listening, listener, port})

      :ok
    end
  end

  @spec listening(String.t()) :: {:quic | :tcp | :web, :inet.port_number()} | nil
  defp listening(line) do
    patterns = [
      quic: ~r/ listening addr=\S+:(\d+) kind="quic"/,
      web: ~r/ listening addr=\S+:(\d+) kind="http"/,
      tcp: ~r/ listening \(tcp\) addr=\S+:(\d+)/
    ]

    Enum.find_value(patterns, fn {listener, pattern} ->
      with [port] <- Regex.run(pattern, line, capture: :all_but_first),
           do: {listener, String.to_integer(port)}
    end)
  end

  @spec output(Options.t(), String.t()) :: :ok
  defp output(options, line) do
    case options.output do
      nil -> :ok
      fun when is_function(fun, 1) -> fun.(line)
      level -> Logger.log(level, "moq-relay: " <> line)
    end

    :ok
  end

  # A line's level follows its timestamp: `2024-01-01T00:00:00Z  WARN ...`.
  # Lines without one, like the ones of an error the relay exits with, are
  # always within it.
  @spec within_level?(String.t(), Relay.log_level()) :: boolean()
  defp within_level?(line, log_level) do
    with [_timestamp, level, _rest] <- String.split(line, " ", parts: 3, trim: true),
         index when is_integer(index) <- Options.level_index(String.downcase(level)) do
      index <= Options.level_index(log_level)
    else
      _no_level -> true
    end
  end

  @spec stop_daemon(pid()) :: :ok
  defp stop_daemon(daemon) do
    GenServer.stop(daemon)
  catch
    :exit, _reason -> :ok
  end
end
