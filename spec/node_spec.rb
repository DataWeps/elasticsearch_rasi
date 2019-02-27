require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'elasticsearch'

describe ElasticsearchRasi do
  let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
  let(:ids)   { %w[1] }
  let(:data)  { %w[titulek] }

  describe 'save node' do
    after { delete_ids(klass, ids) }
    subject do
      klass.node.save_document(docs: {
        '_id'     => '1',
        'title'   => 'titulek',
        'content' => 'titulek obsah' })[:ok]
    end

    it { is_expected.to be_truthy }
  end

  describe 'read node' do
    before do
      create_ids(klass, ids, data)
      delete_ids(klass, ids)
    end

    subject { klass.node.get_docs(ids: ids) }

    it { is_expected.not_to eq({}) }
  end

  describe 'read node ids' do
    subject { klass.node.get_ids(ids: [1]) }
    it { is_expected.to include(*ids) }
  end
end
