require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'
require 'elasticsearch'

describe ElasticsearchRasi do
  context 'Direct idx' do
    before(:context) do
      @es_opts = {
        direct_idx: true,
        idx_write: 'index',
        idx_read:  'index',
        type:      'document',
        connect: { url: 'localhost:9200', index: 'rss_feeds' } }
      @rasi_es = ElasticsearchRasi.new(:rss_feeds, @es_opts)
      @es = Elasticsearch::Client.new(@rasi_es.config[:connect])
      @es.delete(
        index: @rasi_es.config[:idx_write],
        id:    '1',
        type:  @rasi_es.config[:type],
        ignore: 404)
      @es.indices.refresh(
        index: @rasi_es.config[:idx_write]
      )
    end

    it 'check config' do
      expect(@rasi_es.config.size).not_to eq(0)
    end

    it 'save node' do
      expect(@rasi_es.document.save_document(
        '_id'     => 1,
        'title'   => 'ahoj',
        'content' => 'ahoj obsah')).to eq(true)
      @es.indices.refresh(
        index: @rasi_es.config[:idx_mention_write])
      expect(@rasi_es.document.get_document(['1'])['1']['title']).to eq('ahoj')
    end

    after(:context) do
      @es.delete(
        index: @rasi_es.config[:idx_write],
        id:    '1',
        type:  @rasi_es.config[:type],
        ignore: 404)
    end
  end
end
