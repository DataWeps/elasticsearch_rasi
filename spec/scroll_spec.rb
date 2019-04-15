require 'spec_helper'
require 'elasticsearch'

describe ElasticsearchRasi do
  before do
    ES[:disputatio][:mention_write_date] = true
    ES[:disputatio][:mention_read_date]  = true
    ES[:disputatio][:mention_max_age]    = 3
  end

  let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
  let(:es)    { Elasticsearch::Client.new(klass.config[:connect]) }
  before { create_ids(klass, ids, ids) }
  after  { delete_ids(klass, ids) }
  let(:ids) { (1..100).map(&:to_s) }

  context 'scroll' do
    let(:searched_ids) { %w[1 2 3 4 5 6 7] }
    subject do
      docs = {}
      query = klass.mention.get_bool_query(terms: { content: searched_ids })
      query['size'] = 1
      klass.mention.scroll(query: query) do |document|
        docs[document['_id']] = document['_source']
      end
      docs
    end

    it { expect(subject.keys).to include(*searched_ids) }
  end
end
