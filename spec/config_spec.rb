require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'elasticsearch'

describe ElasticsearchRasi do
  context 'read from specific date' do
    before :context do
      ES[:disputatio][:mention_read_date] = true
      ES[:disputatio][:mention_read_date_base] = 'disputatio_mentions'
      ES[:disputatio][:mention_max_age] = 6
    end

    context 'save node' do
      subject(:es) { ElasticsearchRasi.new(:disputatio) }

      it 'should has 6 months' do
        expect(es.mention.read_date_months.size).to be(ES[:disputatio][:mention_max_age] + 1)
      end
    end
  end

  context 'read from current index' do
    before :context do
      ES[:disputatio][:mention_read_date] = false
      ES[:disputatio][:mention_max_age] = 6
    end

    context 'save node' do
      subject(:es) { ElasticsearchRasi.new(:disputatio) }

      it 'should has 6 months' do
        expect(es.mention.read_date_months).to be_empty
      end

      it 'should has read date false' do
        expect(es.mention.read_date).to be_falsey
      end
    end
  end
end
