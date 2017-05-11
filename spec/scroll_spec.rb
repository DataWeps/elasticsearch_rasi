require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'
require 'elasticsearch'

describe ElasticsearchRasi do
  context 'initialize from config' do
    before(:context) do
      @rasi_es = ElasticsearchRasi.new(:disputatio)
      @es = Elasticsearch::Client.new(@rasi_es.config[:connect])
      @saves = { create: [], delete: [] }
      (1..100).each do |id|
        @saves[:create] << {
          create: {
            _index: @rasi_es.config[:idx_node_write],
            _id:    id,
            _type:  @rasi_es.config[:node_type],
            data:   { url: 'test' }
          }
        }
        @saves[:delete] << {
          delete: {
            _index: @rasi_es.config[:idx_node_write],
            _id:    id,
            _type:  @rasi_es.config[:node_type]
          }
        }
      end
      @es.send(:bulk, body: @saves[:delete])
      @es.indices.refresh(index: @rasi_es.config[:idx_node_write])
      @es.send(:bulk, body: @saves[:create])
      @es.indices.refresh(index: @rasi_es.config[:idx_node_write])
    end

    it 'read scroll total' do
      ids = []
      @rasi_es.node.scroll(
        { query: { bool: { filter: { term: { url: 'test' } } } } }, { scroll: '5m' }) do |d|
        ids << d['_id']
      end
      @rasi_es.mention.scroll( { query: { bool: { filter: {} } } }, { scroll: '5m' }) {}
      expect(ids.size).to eql(@saves[:create].size)
    end

    it 'read scroll fields' do
      urls = []
      @rasi_es.node.scroll(
        _source: ['url'],
        query: { bool: { filter: { term: { url: 'test' } } } }
      ) do |d|
        urls << d['_source']['url']
      end
      urls.keep_if { |url| url.match(/test/) }
      expect(urls.size).to eql(@saves[:create].size)
    end

    after(:context) do
      @es.send(:bulk, body: @saves[:delete])
    end
  end
end
