require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'elasticsearch'

describe ElasticsearchRasi do
  def test_count
    klass.mention.count(
      query: {
        bool: {
          filter: {
            term: {
              resource: 'zpravy.idnes'
            }
          }
        }
      }
    )
  end

  context 'initialize from config' do
    let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
    let(:es)    { Elasticsearch::Client.new(klass.config[:connect]) }
    let(:id) { 'test_abc' }

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

    it 'direct config' do
      expect(rasi_es.config[:connect][:host]).to eq(ES[:disputatio][:connect][:host])
    end

    context 'direct_idx config' do
      let(:rasi_es)do
        ElasticsearchRasi::Client.new(:example,
          direct_idx: true,
          idx_write: 'index',
          idx_read:  'index',
          connect: { url: 'localhost:9200' })
      end

      it 'should has config with right index' do
        expect(rasi_es.config[:idx_write]).to eq('index')
      end
    end

    context 'count' do
      it 'should be zero size' do
        expect(test_count).to be(0)
      end
    end

    context 'count and save document' do
      before do
        klass.mention.save_document(_id: id, resource: 'zpravy.idnes')
        klass.mention.refresh
      end

      it 'should has size 1' do
        expect(test_count).to be(1)
      end
    end

    context 'update document' do
      subject { klass.mention.get(id: @id) }
      before do
        es.index(
          id: id,
          type: klass.config[:mention_type],
          index: klass.config[:idx_mention_write], body: {
          resource: 'zpravy.idnes',
          content: 'a' })
        klass.mention.refresh
        klass.mention.update_document('_id' => @id, content: 'different content')
        klass.mention.refresh
      end

      it { is_expected.to be('different content') }
    end

    after do
      es.delete(
        index: klass.config[:idx_mention_write],
        id:    id,
        type:  klass.config[:mention_type],
        ignore: 404)
    end
  end
end
