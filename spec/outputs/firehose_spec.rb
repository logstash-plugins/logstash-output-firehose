# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/firehose"
require "logstash/codecs/plain"
require "logstash/codecs/line"
require "logstash/codecs/json_lines"
require "logstash/event"
require "aws-sdk"

describe LogStash::Outputs::Firehose do
  dataStr = "123,someValue,1234567890"

  let(:sample_event) { LogStash::Event.new("message" => dataStr) }
  let(:expected_event) { LogStash::Event.new("message" => dataStr) }
  let(:output) { LogStash::Outputs::Firehose.new({"codec" => "plain"}) }

  before do
    Thread.abort_on_exception = true

    # Setup Firehose client
    output.stream = "aws-test-stream"
    output.access_key_id = "Key ID"
    output.secret_access_key = "Secret key"
    output.register
  end

  describe "receive message with plain codec" do
    subject {
      expect(output).to receive(:handle_event) do |arg|
        arg
      end
      output.receive(sample_event)
    }

    it "returns same string" do
      expect(subject).not_to eq(nil)
      expect(subject.include? expected_event["message"]).to be_truthy
      # expect(subject).to eq(expected_event["message"])
    end
  end
end
