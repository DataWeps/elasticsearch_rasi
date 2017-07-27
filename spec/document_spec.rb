require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'
require 'elasticsearch_rasi'

describe ElasticsearchRasi do
  context 'methods' do
    before(:context) do
      @rasi_es = ElasticsearchRasi.new(:disputatio)
    end

    context 'prepare_search_author' do
      before :context do
        @doc = { 'author' => 'Pepa' }
        @rasi_es.prepare_search_author!(@doc)
      end

      it 'should has search_author field' do
        expect(@doc['search_author']['name']).to eq('Pepa')
      end
    end
  end
end
