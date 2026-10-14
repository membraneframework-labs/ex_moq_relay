defmodule ExMoQ.Relay.InfoTest do
  use ExUnit.Case, async: true

  alias ExMoQ.Relay
  alias ExMoQ.Relay.Info
  alias ExMoQ.Relay.Options

  test "a listener is reached on its address, loopback for an unspecified one; one that is off is nil" do
    options =
      Options.validate!(%Relay{
        ip: {0, 0, 0, 0},
        quic: {4443, tls_generate: "localhost"},
        internal: {9101, ip: {0, 0, 0, 0, 0, 0, 0, 0}}
      })

    assert Info.new(options) == %Info{
             quic: {"https://127.0.0.1:4443", :generated},
             tcp: nil,
             web: nil,
             internal: "http://[::1]:9101"
           }

    options =
      Options.validate!(%Relay{
        ip: {0, 0, 0, 0, 0, 0, 0, 0},
        quic: {4443, tls_generate: nil, ip: {192, 0, 2, 1}},
        tcp: 1
      })

    assert %Info{quic: {"https://192.0.2.1:4443", :provided}, tcp: "tcp://[::1]:1"} =
             Info.new(options)
  end
end
