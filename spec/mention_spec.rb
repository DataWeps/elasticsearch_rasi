require 'spec_helper'

require 'active_support/core_ext/time/calculations'
require 'webmock/rspec'

describe 'Mention' do
  WebMock.allow_net_connect!
  let(:klass) { ElasticsearchRasi::Client.new(:disputatio) }

  describe 'save to specific date' do
    before do
      ES[:disputatio][:mention_write_date] = true
      ES[:disputatio][:mention_max_age] = 6
    end

    context 'save node' do
      subject do
        klass.mention.create_bulk(
          [
            { '_id' => 'test',
              'published_at' => Time.now.strftime('%Y-%m-%d') }],
          klass.mention.config[:idx_write])
      end

      it 'should has date in to now' do
        expect(subject[0][:index][:_index]).to \
          match(/#{Regexp.escape(Time.now.strftime('%Y%m'))}/)
      end
    end

    context 'skip_too_old_mentions' do
      let(:data) do
        [{
            '_id'          => 'too_old',
            'published_at' => Time.now.months_ago(7).beginning_of_month.to_s },
          {
            '_id'          => 'also_too_old',
            'published_at' => (Time.now.months_ago(6).beginning_of_month - 86_400).to_s },
         {
           '_id'          => 'just enough',
           'published_at' => Time.now.months_ago(6).beginning_of_month.to_s }]
      end
      subject { klass.mention.create_bulk(data, klass.mention.config[:idx_write]) }

      it { expect(subject.size).to be(1) }
    end
  end

  describe 'save to specific language' do
    let(:url) { 'localhost:9203' }
    before do
      ES[:disputatio][:mention_lang_index] = true
      ES[:disputatio][:mention_write_date] = true
      ES[:disputatio][:mention_max_age] = 6
      ES[:disputatio][:connect_another] = [{ connect: { host: url } }]
    end

    subject do
      klass.mention.save_document(docs:
        [
          { '_id' => 'test',
            'languages' => ['cs'],
            'published_at' => Time.now.strftime('%Y-%m-%d') }])
    end

    it 'should has create search_author field' do
      expect(subject).to have_requested(:post, /_bulk/).with { |request|
        response = ElasticsearchRasi::JsonHelper.load(request.body.split("\n")[-1])
        response.include?('search_author') && response['search_author'].include?('author_hash')
      }
    end
  end
end
