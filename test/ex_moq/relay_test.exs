defmodule ExMoQ.RelayTest do
  use ExUnit.Case

  import ExMoQ.Testing.Relay, only: [free_port: 0]

  alias ExMoQ.Relay
  alias ExMoQ.Relay.Info

  @fake_silent """
  #!/bin/sh
  sleep 60
  """

  # Prints where readiness is reported, and exits cleanly soon after.
  @fake_clean_exit """
  #!/bin/sh
  echo "$NOTIFY_SOCKET"
  sleep 0.3
  """

  defp script!(dir, name, body) do
    path = Path.join(dir, name)
    File.write!(path, body)
    File.chmod!(path, 0o755)
    path
  end

  @tag :tmp_dir
  test "a relay that never reports readiness is stopped, and exits the caller", %{tmp_dir: dir} do
    options = %Relay{binary: script!(dir, "silent", @fake_silent), ready_timeout: 300}

    {caller, ref} = spawn_monitor(fn -> Relay.start_link(options) end)
    assert_receive {:DOWN, ^ref, :process, ^caller, {:not_ready, 300}}, 1_000
  end

  @tag :tmp_dir
  test "a relay that exits cleanly once ready stops with exit status 0", %{tmp_dir: dir} do
    ready = fn path ->
      {:ok, socket} = :gen_udp.open(0, [:local])
      :ok = :gen_udp.send(socket, {:local, String.trim(path)}, 0, "READY=1")
      :gen_udp.close(socket)
    end

    options = %Relay{binary: script!(dir, "clean", @fake_clean_exit), output: ready}
    {:ok, relay} = Relay.start(options)
    ref = Process.monitor(relay)

    assert_receive {:DOWN, ^ref, :process, ^relay, {:exit_status, 0}}, 1_000
  end

  describe "a real moq-relay" do
    @describetag :integration

    test "starts with every listener, reports a version, and stops" do
      assert {:ok, version} = Relay.version()
      assert version =~ ~r/^\d+\.\d+\.\d+/

      [quic, tcp, internal] = Enum.map(1..3, fn _n -> free_port() end)

      options = %Relay{
        quic: quic,
        tcp: tcp,
        web: quic,
        internal: internal,
        name: __MODULE__.Relay,
        output: nil
      }

      {:ok, relay} = Relay.start_link(options)

      assert Process.whereis(__MODULE__.Relay) == relay

      info = Relay.info(relay)
      assert info.quic_url == "https://127.0.0.1:#{quic}"
      assert info.tcp_url == "tcp://127.0.0.1:#{tcp}"
      assert info.web_url == "http://127.0.0.1:#{quic}"
      assert info.internal_url == "http://127.0.0.1:#{internal}"
      assert info.tls == :generated

      assert {200, _body} = get(info.internal_url <> "/health")
      assert {200, _body} = get(info.web_url <> "/health")
      # The SHA-256 of the generated certificate, hex-encoded.
      assert {200, fingerprint} = get(info.web_url <> "/certificate.sha256")
      assert String.trim(fingerprint) =~ ~r/^[0-9a-f]{64}$/

      {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", tcp, [:binary])
      :gen_tcp.close(socket)

      assert :ok = Relay.stop(__MODULE__.Relay)
      refute Process.alive?(relay)
    end

    test "logs at :log_level despite a RUST_LOG in the environment" do
      previous = System.get_env("RUST_LOG")
      System.put_env("RUST_LOG", "trace")

      on_exit(fn ->
        if previous, do: System.put_env("RUST_LOG", previous), else: System.delete_env("RUST_LOG")
      end)

      me = self()

      options =
        %Relay{quic: nil, tcp: free_port(), log_level: :error, output: &send(me, {:line, &1})}

      start_supervised!({Relay, options})
      refute_receive {:line, _line}, 500
    end

    test "binds a listener to an :ip of its own" do
      web = free_port()
      options = %Relay{quic: nil, tcp: free_port(), web: {web, ip: {0, 0, 0, 0}}, output: nil}

      info = Relay.info(start_supervised!({Relay, options}))
      assert info.web_url == "http://127.0.0.1:#{web}"
      assert {200, _body} = get(info.web_url <> "/health")
    end

    test "a relay that binds its listeners but cannot authenticate is not ready" do
      options = %Relay{tcp: free_port(), auth: nil, output: nil}
      assert {:error, {:exit_status, 1}} = Relay.start(options)

      options = %Relay{options | auth: [subscribe: "**"]}
      assert %Info{} = Relay.info(start_supervised!({Relay, options}))
    end
  end

  defp get(url) do
    {:ok, {{_version, status, _reason}, _headers, body}} =
      :httpc.request(:get, {String.to_charlist(url), []}, [timeout: 2_000], body_format: :binary)

    {status, body}
  end
end
