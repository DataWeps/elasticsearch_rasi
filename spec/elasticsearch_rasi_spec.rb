require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'

describe ElasticsearchRasi do
  context 'initialize from config' do
    before(:context) do
      @es = ElasticsearchRasi.new(:disputatio)
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
        "#{ES[:disputatio][:base]}#{ES[:disputatio][:node_suffix]}" \
        "#{ES[:disputatio][:node_write]}")
      expect(@es.config[:idx_node_read]).to eq(
        "#{ES[:disputatio][:base]}#{ES[:disputatio][:node_suffix]}" \
        "#{ES[:disputatio][:node_read]}")
    end

    it 'check mention index' do
      expect(@es.config[:idx_mention_write]).to eq(
        "#{ES[:disputatio][:base]}#{ES[:disputatio][:mention_suffix]}" \
        "#{ES[:disputatio][:mention_write]}")
      expect(@es.config[:idx_mention_read]).to eq(
        "#{ES[:disputatio][:base]}#{ES[:disputatio][:mention_suffix]}" \
        "#{ES[:disputatio][:node_read]}")
    end

    it 'check client index' do
      expect(@es.config[:idx_node_read_client]).to eq(
        "#{ES[:disputatio][:node_client]}")
      expect(@es.config[:idx_mention_read_client]).to eq(
        "#{ES[:disputatio][:mention_client]}")
    end
  end
end
