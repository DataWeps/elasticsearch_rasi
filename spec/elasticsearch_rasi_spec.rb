require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'

describe ElasticsearchRasi do
  context 'initialize from config' do
    before(:context) do
      @key = :forums
      @es = ElasticsearchRasi.new(@key)
    end

    it 'check config' do
      expect(@es.config.size).not_to eq(0)
    end

    it 'check nil config' do
      expect do
        ElasticsearchRasi.new(:forums)
      end.to raise_error(ArgumentError)
    end

    it 'check node index' do
      expect(@es.config[:idx_node_write]).to eq(
        "#{ES[@key][:base]}#{ES[@key][:node_suffix]}" \
        "#{ES[@key][:node_write]}")
      expect(@es.config[:idx_node_read]).to eq(
        "#{ES[@key][:base]}#{ES[@key][:node_suffix]}" \
        "#{ES[@key][:node_read]}")
    end

    it 'check mention index' do
      expect(@es.config[:idx_mention_write]).to eq(
        "#{ES[@key][:base]}#{ES[@key][:mention_suffix]}" \
        "#{ES[@key][:mention_write]}")
      expect(@es.config[:idx_mention_read]).to eq(
        "#{ES[@key][:base]}#{ES[@key][:mention_suffix]}" \
        "#{ES[@key][:node_read]}")
    end

    it 'check client index' do
      expect(@es.config[:idx_node_read_client]).to eq(
        "#{ES[@key][:node_client]}")
      expect(@es.config[:idx_mention_read_client]).to eq(
        "#{ES[@key][:mention_client]}")
    end
  end
end
