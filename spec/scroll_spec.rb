require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'elasticsearch'

describe ElasticsearchRasi do
  let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }
  let(:es)    { Elasticsearch::Client.new(klass.config[:connect]) }
  before { create_ids(ids) if defined?(ids) }
  after  { delete_ids(ids) if defined?(ids) }
  let(:ids) { [1..100].map(&:to_s) }

  context 'scroll' do
  end
end
