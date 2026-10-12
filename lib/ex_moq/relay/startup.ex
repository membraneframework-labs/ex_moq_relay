defmodule ExMoQ.Relay.Startup do
  @moduledoc false
  # Starts the moq-relay OS process and waits until it is ready: NOTIFY_SOCKET
  # reports READY=1, and any `:auto` ports are resolved from listening log lines.
  # Called from `ExMoQ.Relay` init; the GenServer is the mailbox for notify and
  # scraped ports, and stays linked to the daemon.

  require Logger

  alias ExMoQ.Relay.{Args, Options}

  @type t :: %{
          daemon: pid(),
          monitor: reference(),
          options: Options.t()
        }

  @spec run(Options.t()) :: {:ok, t()} | {:error, term()}
  def run(%Options{} = options) do
    deadline = System.monotonic_time(:millisecond) + options.ready_timeout
    path = notify_path()
    File.rm(path)
    {:ok, notify} = :gen_udp.open(0, [:local, :binary, active: true, ifaddr: {:local, path}])

    daemon_opts = [
      stderr_to_stdout: true,
      exit_status_to_reason: &{:exit_status, &1},
      logger_fun: logger_fun(options, self()),
      env: env(options, path)
    ]

    # The daemon's init fails only when the validated binary is gone, muontrap's
    # own binary is missing, or the VM is out of file descriptors. Being linked,
    # that failure exits this process with its reason rather than returning
    # `{:error, _}`, which is as it should be: none of those are handled.
    {:ok, daemon} = MuonTrap.Daemon.start_link(options.binary, Args.args(options), daemon_opts)
    state = %{options: options, daemon: daemon, monitor: Process.monitor(daemon)}

    result =
      case await_ready(state, notify, false, deadline) do
        {:ok, state} ->
          {:ok, Map.take(state, [:daemon, :monitor, :options])}

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

  @spec env(Options.t(), Path.t()) :: [{String.t(), String.t()}]
  defp env(options, notify_path) do
    directives =
      if raise_level?(options),
        do: Enum.map(["moq_relay::relay", "moq_relay::web", "moq_tokio::server"], &"#{&1}=info"),
        else: []

    [
      {"NOTIFY_SOCKET", notify_path},
      {"NO_COLOR", "1"},
      {"RUST_LOG", Enum.join([options.log_level | directives], ",")}
    ]
  end

  @spec raise_level?(Options.t()) :: boolean()
  defp raise_level?(options),
    do:
      not resolved?(options) and
        Options.level_index(options.log_level) < Options.level_index("info")

  @spec await_ready(map(), :gen_udp.socket(), boolean(), integer()) ::
          {:ok, map()} | {:error, term()}
  defp await_ready(state, notify, notified?, deadline) do
    if notified? and resolved?(state.options) do
      {:ok, state}
    else
      monitor = state.monitor
      timeout = max(deadline - System.monotonic_time(:millisecond), 0)

      receive do
        {:udp, ^notify, _address, _port, message} ->
          ready? = "READY=1" in String.split(message, "\n")
          await_ready(state, notify, notified? or ready?, deadline)

        {:listening, listener, port} ->
          options = resolve(state.options, listener, port)
          await_ready(%{state | options: options}, notify, notified?, deadline)

        {:DOWN, ^monitor, :process, _daemon, :normal} ->
          {:error, {:exit_status, 0}}

        {:DOWN, ^monitor, :process, _daemon, reason} ->
          {:error, reason}
      after
        timeout -> {:error, {:not_ready, state.options.ready_timeout}}
      end
    end
  end

  @spec resolved?(Options.t()) :: boolean()
  defp resolved?(options),
    do: not Enum.any?([:quic, :tcp, :web], &match?({:auto, _ip}, Args.listener(options, &1)))

  @spec resolve(Options.t(), :quic | :tcp | :web, :inet.port_number()) :: Options.t()
  defp resolve(options, listener, port) do
    case Map.fetch!(options, listener) do
      :auto -> Map.replace!(options, listener, port)
      {:auto, opts} -> Map.replace!(options, listener, {port, opts})
      _other -> options
    end
  end

  # Runs in the daemon's process, so the lines a relay prints as it exits are
  # written out before the exit reaches the relay's process.
  @spec logger_fun(Options.t(), pid()) :: (String.t() -> :ok)
  defp logger_fun(options, server) do
    filter_level? = raise_level?(options)
    resolve? = not resolved?(options)

    fn line ->
      if not filter_level? or within_level?(line, options.log_level), do: output(options, line)

      with true <- resolve?,
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
  @spec within_level?(String.t(), String.t()) :: boolean()
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
