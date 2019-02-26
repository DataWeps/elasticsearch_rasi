require 'spec_helper'
require 'elasticsearch'

describe ElasticsearchRasi do
  subject { ElasticsearchRasi::Client.new(:disputatio) }

  describe 'read from specific date' do
    before :context do
      ES[:disputatio][:mention_read_date] = true
      ES[:disputatio][:mention_read_date_base] = 'disputatio_mentions'
      ES[:disputatio][:mention_max_age] = 6
    end

    context 'save node' do
      it 'should has 6 months' do
        expect(subject.mention.read_date_months.size).to be(ES[:disputatio][:mention_max_age] + 1)
      end
    end
  end

  describe 'read from current index' do
    before :context do
      ES[:disputatio][:mention_read_date] = false
      ES[:disputatio][:mention_max_age] = 6
    end

    context 'save node' do
      it 'should has 6 months' do
        expect(subject.mention.read_date_months).to be_empty
      end

      it 'should has read date false' do
        expect(subject.mention.read_date).to be_falsey
      end
    end
  end

  describe 'initialize from config' do
    let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
    let(:es)    { Elasticsearch::Client.new(klass.config[:connect]) }
    let(:id)    { 'test_abc' }

    before do
      es.delete(
        index: klass.config[:idx_mention_write],
        id:    id,
        type:  klass.config[:mention_type],
        ignore: 404)
      klass.mention.refresh
    end

    it 'check config' do
      expect(klass.config.size).not_to eq(0)
    end

    it 'should has correct host' do
      expect(klass.config[:connect][:host]).to eq(ES[:disputatio][:connect][:host])
    end
  end

  describe 'Direct idx' do
    let(:es_opts) do
      {
        direct_idx: true,
        idx_write: 'test_articles',
        idx_read:  'test_articles',
        type:      'document',
        connect: { url: ES[:disputatio][:connect][:host], index: 'test_articles' } }
    end
    let(:klass) { ElasticsearchRasi::Client.new(:rss_feeds, es_opts) }
    let(:es) { Elasticsearch::Client.new(url: ES[:disputatio][:connect][:host]) }

    before do
      es.delete(
        index: klass.config[:idx_write],
        id:    '1',
        type:  klass.config[:type],
        ignore: 404)
      es.indices.refresh(
        index: klass.config[:idx_write])
    end

    describe 'check config' do
      subject { klass.config.size }
      it { is_expected.not_to eq(0) }
    end

    describe 'save node' do
      let(:data) do
        {
          '_id'     => 1,
          'title'   => 'ahoj',
          'content' => 'ahoj obsah' }
      end
      subject { klass.document.save_document(data) }
      it { is_expected.to eq(true) }

      context 'saved_data' do
        before do
          es.indices.refresh(index: klass.config[:idx_mention_write])
        end
        subject { klass.document.get(id: '1')['title'] }
        it { is_expected.to eq('ahoj') }
      end
    end

    after do
      es.delete(
        index: klass.config[:idx_write],
        id:    '1',
        type:  klass.config[:type],
        ignore: 404)
    end
  end
end
