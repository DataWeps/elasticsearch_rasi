require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'
require 'elasticsearch'
require 'active_support/core_ext/time/calculations'

describe ElasticsearchRasi do
  context 'search' do
    let(:es) do
      ElasticsearchRasi.new(:disputatio)
    end

    let(:result) do
      es.mention.search(
        Oj.load(%({"size": 1, "query": { "filtered": { "filter": {}}}})))
    end

    it 'should has search' do
      expect(result.size).not_to be(0)
    end
  end

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

    it 'save mention' do
      expect(@rasi_es.mention.save_document(
        '_id'     => '1',
        'title'   => 'titulek',
        'author'  => 'pokus',
        'content' => 'titulek obsah')[:ok]).to eq(true)
      @es.indices.refresh(
        index: @rasi_es.config[:idx_mention_write])
    end

    it 'save more mentions' do
      expect(@rasi_es.mention.save_document([
        {
          '_id'     => 1,
          'title'   => 'titulek',
          'content' => 'titulek obsah' },
        {
          '_id'     => 2,
          'title'   => 'titulek',
          'content' => 'titulek obsah' }])[:ok]).to eq(true)
    end

    context :error_mentions do
      let(:response) do
        @rasi_es.mention.save_document([
          {
            '_id'     => 1,
            'title'   => 'titulek',
            'cc'   => 'error',
            'content' => 'titulek obsah' },
          {
            '_id'     => 2,
            'title'   => 'titulek',
            'content' => 'titulek obsah' }])
      end

      it 'read mention' do
        expect(response[:ok]).not_to be(true)
      end
    end

    it 'exists mention' do
      expect(@rasi_es.mention.get_ids([1])[0]).to eq("1")
    end

    it 'delete mention' do
      response = @rasi_es.mention.save_document({ '_id' => 1 }, :delete)
      expect(response[:ok]).to be(true)
    end

    after(:context) do
      # @es.delete(
      #   index: @rasi_es.config[:idx_mention_write],
      #   id:    '1',
      #   type:  @rasi_es.config[:mention_type],
      #   ignore: 404)
      # @es.delete(
      #   index: @rasi_es.config[:idx_mention_write],
      #   id:    '2',
      #   type:  @rasi_es.config[:mention_type],
      #   ignore: 404)
    end
  end

  context 'save to specific date' do
    before :context do
      ES[:disputatio][:mention_write_date] = true
      ES[:disputatio][:mention_max_age] = 6
      @es = ElasticsearchRasi.new(:disputatio)
    end

    context 'save node' do
      before :context do
        @bulk = @es.mention.create_bulk([{'_id' => 'test', 'published_at' => '2017-01-05'}], @es.mention.config[:idx_write])
      end

      it 'should has date in 201701' do
        expect(@bulk[0][:index][:_index]).to match(/201705/)
      end
    end
  end

  context :prepare_index do
    context :skip_too_old_mentions do
      before :context do
        ES[:disputatio][:mention_write_date] = true
        ES[:disputatio][:mention_max_age] = 6
        @es = ElasticsearchRasi.new(:disputatio)
      end

      let(:data) do
        [{
          '_id'          => 'too_old',
          'published_at' => Time.now.months_ago(7).beginning_of_month.to_s },
         {
           '_id'          => 'just enough',
           'published_at' => Time.now.months_ago(6).beginning_of_month.to_s }]
      end

      let(:bulk) do
        @es.mention.create_bulk(data, @es.mention.config[:idx_write])
      end

      it 'should has just one bulk' do
        expect(bulk.size).to be(1)
      end
    end
  end
end
