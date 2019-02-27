require 'spec_helper'
require 'util/json_helper'

require 'elasticsearch'
require 'webmock/rspec'

describe ElasticsearchRasi do
  WebMock.allow_net_connect!
  let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
  let(:es)    { Elasticsearch::Client.new(klass.config[:connect]) }
  before { delete_ids(klass, %w[test]) }
  subject { klass.mention.save_document(docs: document) }

  describe 'save_docs' do
    shared_examples 'should_save_data' do
      it { expect(subject[:ok]).to be_truthy }

      it 'should has create search_author field' do
        expect(subject).to have_requested(:post, /_bulk/).with { |request|
          response = ElasticsearchRasi::JsonHelper.load(request.body.split("\n")[-1])
          response.include?('search_author') && response['search_author'].include?('author_hash')
        }
      end
    end

    context '{data}' do
      let(:document) do
        {
          '_id'     => 'test',
          'content' => 'content that',
          'author'  => 'author surname',
          'published_at' => '2019-01-01' }
      end

      it_behaves_like('should_save_data')
    end

    context '[{data}]' do
      let(:document) do
        [{
          '_id'     => 'test',
          'content' => 'content that',
          'author'  => 'author surname',
          'published_at' => '2019-01-01' }]
      end

      it_behaves_like('should_save_data')
    end

    context '{id => data}' do
      let(:document) do
        { 'test' => {
            'content' => 'content that',
            'author'  => 'author surname',
            'published_at' => '2019-01-01' },
          'test_2' => {
            'content' => 'content that',
            'author'  => 'author surname',
            'published_at' => '2019-01-01' } }
      end

      it_behaves_like('should_save_data')
    end

    describe 'wrong data' do
      shared_examples 'should_raise_error' do
        it { expect { subject }.to raise_error(TypeError) }
      end

      context 'wrong HashType' do
        let(:document) do
          {
            'content' => 'content that',
            'author'  => 'author surname',
            'published_at' => '2019-01-01' }
        end

        it_behaves_like('should_raise_error')
      end

      context 'wrong type at all' do
        let(:document) do
          "content"
        end

        it_behaves_like('should_raise_error')
      end
    end
  end
end
