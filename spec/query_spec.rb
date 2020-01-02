require 'spec_helper'

describe 'Query' do
  let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
  let(:es)    { Elasticsearch::Client.new(klass.config[:connect]) }
  before { create_ids(klass, ids) if defined?(ids) }
  after  { delete_ids(klass, ids) if defined?(ids) }

  describe 'query_docs_by_###' do
    let(:ids) { %w[test] }

    describe 'get docs' do
      shared_examples 'should_has_data' do
        subject { klass.mention.get_docs(ids: ids) }
        it 'should contains test object' do
          expect(subject['test']['content']).to eq('0')
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

  describe 'get' do
    let(:ids) { %w[test] }
    context 'just document' do
      subject { klass.mention.get(id: ids[0])['content'] }
      it { is_expected.to eq('0') }
    end

    context 'return complete document' do
      subject { klass.mention.get(id: ids[0], just_source: false) }
      it { is_expected.to include(*ids) }
    end

    context 'return specific source' do
      subject { klass.mention.get(id: ids[0], source: %w[content]) }
      it { is_expected.to eq("content" => "0") }
    end
  end

  describe 'get_ids' do
    let(:ids) { %w[test_1 test_2 test_3] }
    shared_examples 'should_contains_ids' do
      subject { klass.mention.get_ids(ids: ids) }
      it 'should contains ids' do
        expect(subject).to include(*ids)
      end
    end

    context 'by_filter' do
      before { klass.mention.config[:alias] = true }
      it_behaves_like('should_contains_ids')
    end

    context 'by_mget' do
      before { klass.mention.config[:alias] = false }
      it_behaves_like('should_contains_ids')
    end
  end

  describe 'search' do
    context 'ignore_max_age' do
      let(:es) { ElasticsearchRasi::Client.new(:disputatio, ignore_max_age: true) }

      subject do
        es.mention.search(
          query: klass.mention.get_bool_query(match: { content: 'that' }))
      end

      it { is_expected.to be(true) }
    end

    context 'not ima' do
      before do
        es.index(
          index: klass.mention.config[:idx_read],
          type:  klass.mention.config[:type],
          body: { content: 'content this', resource: 'resource' },
          id:   'test_1')
        es.index(
          index: klass.mention.config[:idx_read],
          type:  klass.mention.config[:type],
          body: { content: 'content that', resource: 'resource' },
          id:   'test_2')
        es.indices.refresh(index: klass.mention.config[:idx_read])
      end

      subject do
        klass.mention.search(query: klass.mention.get_bool_query(match: { content: 'that' }))
      end

      it { is_expected.to include('test_2') }
      it { is_expected.not_to include('test_1') }
    end
  end

  describe 'search' do
    context 'proper search' do
      before do
        es.index(
          index: klass.mention.config[:idx_read],
          type:  klass.mention.config[:type],
          body: { content: 'content this', resource: 'resource' },
          id:   'test_1')
        es.index(
          index: klass.mention.config[:idx_read],
          type:  klass.mention.config[:type],
          body: { content: 'content that', resource: 'resource' },
          id:   'test_2')
        es.indices.refresh(index: klass.mention.config[:idx_read])
      end

      subject do
        klass.mention.search(query: klass.mention.get_bool_query(match: { content: 'that' }))
      end

      it { is_expected.to include('test_2') }
      it { is_expected.not_to include('test_1') }
    end

    context 'wrong query' do
      subject do
        klass.mention.search(query: klass.mention.get_bool_query(mtch: { content: 'that' }))
      end

      it { expect { subject }.to raise_error { ParseResponseError } }
    end
  end
end
