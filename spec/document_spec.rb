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

  describe 'write_date' do
    context 'max age is present' do
      before(:context) do
        ES[:disputatio][:node_write_date] = true
        ES[:disputatio][:node_max_age] = 3
      end

      subject(:rasi_es) do
        ElasticsearchRasi.new(:disputatio)
      end

      it 'should has initialized write_date' do
        expect(rasi_es.node.write_date).to be(true)
      end

      it 'should has max age older than' do
        expect(rasi_es.node.max_age).to be > (Time.now.months_ago(ES[:disputatio][:node_max_age] + 1).to_i)
      end
    end

    context 'ignore max age' do
      before(:context) do
        ES[:disputatio][:node_write_date] = true
        ES[:disputatio][:node_max_age] = 3
      end

      subject(:rasi_es) do
        ElasticsearchRasi.new(:disputatio, ignore_max_age: true)
      end

      it 'should has initialized write_date' do
        expect(rasi_es.node.write_date).to be(true)
      end

      it 'should has max age older than' do
        expect(rasi_es.node.max_age).to be(nil)
      end
    end

    context 'write_date is disabled' do
      before(:context) do
        %i[node_write_date node_max_age].each do |key|
          ES[:disputatio].delete(key)
        end
      end

      subject(:rasi_es) do
        ElasticsearchRasi.new(:disputatio)
      end

      it 'should has initialized write_date' do
        expect(rasi_es.node.write_date).to be(false)
      end
    end
  end
end
