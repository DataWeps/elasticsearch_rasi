require 'spec_helper'
$LOAD_PATH.unshift(File.join(__dir__, '../../app/workers'))
require 'elasticsearch'

describe ElasticsearchRasi do
  let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }

  context 'save node' do
    subject { klass.translate_lang_to_country(['cs']) }

    it { is_expected.to eq(['cze']) }
  end
end
