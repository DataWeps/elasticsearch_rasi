require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'
require 'elasticsearch'

describe ElasticsearchRasi do
  context 'initialize from config' do
    before(:context) do
      @rasi_es = ElasticsearchRasi.new(:disputatio)
      @es = Elasticsearch::Client.new(@rasi_es.config[:connect])
      @es.delete(
        index: @rasi_es.config[:idx_mention_write],
        id:    '1',
        type:  @rasi_es.config[:mention_type],
        ignore: 404)
      @es.indices.refresh(
        index: @rasi_es.config[:idx_mention_write]
      )
    end

    it 'check config' do
      expect(@rasi_es.config.size).not_to eq(0)
    end

    it 'save node' do
      expect(@rasi_es.mention.save_document(
        '_id'      => 1,
        'title'   => 'titulek',
        'content' => 'titulek obsah')).to eq(true)
      @es.indices.refresh(
        index: @rasi_es.config[:idx_mention_write])
    end

    it 'read node' do
      expect(@rasi_es.mention.get_document([1]).keys[0]).to eq("1")
    end

    it 'exists node' do
      expect(@rasi_es.mention.get_ids([1])[0]).to eq("1")
    end

    after(:context) do
      @es.delete(
        index: @rasi_es.config[:idx_mention_write],
        id:    '1',
        type:  @rasi_es.config[:mention_type],
        ignore: 404)
    end
  end
end
