require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'oj'
require 'elasticsearch'

describe ElasticsearchRasi do
  context 'initialize from config' do
    before(:context) do
      ES[:disputatio][:node_date_write] = true
      @rasi_es = ElasticsearchRasi.new(:disputatio)
    end

    it 'node should has date name index' do
      expect(@rasi_es.node.config[:idx_write]).to \
        match(/#{Regexp.escape(Time.now.strftime('%Y%m'))}/)
    end

    it 'mention shouldnt has date name index' do
      expect(@rasi_es.mention.config[:idx_write]).not_to \
        match(/#{Regexp.escape(Time.now.strftime('%Y%m'))}/)
    end
  end
end
