require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'elasticsearch'

describe ElasticsearchRasi do
  context 'initialize from config' do
    before do
      es.index(
        index: klass.mention.config[:idx_read],
        type:  klass.mention.config[:type],
        body: {},
        id:   'test')
      es.indices.refresh(index: klass.mention.config[:idx_read])
    end

    let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
    let(:es)    { Elasticsearch::Client.new(klass.config[:connect]) }
    let(:id) { 'test_abc' }

    describe 'get docs' do
      shared_examples 'should_has_data' do
        subject { klass.mention.get_docs(ids: %w[test]) }
        it 'should contains test object' do
          expect(subject['test']).to eq({})
        end
      end

      context 'by_filter' do
        it_behaves_like('should_has_data') do
          before do
            klass.mention.config[:alias] = true
          end
        end
      end

      context 'by_mget' do
        before do
          klass.mention.config[:alias] = false
        end

        it_behaves_like('should_has_data')
      end
    end
  end
end
