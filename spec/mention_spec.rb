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

    context 'save mention' do
      subject do
        klass.mention.save_document(docs: [
          { '_id' => 'test',
            'published_at' => Time.now.strftime('%Y-%m-%d') }])
      end

      it 'should has date in to now' do
        expect(subject).to have_requested(:post, /_bulk/).with { |request|
          response = ElasticsearchRasi::JsonHelper.load(request.body.split("\n")[0])
          response['index']['_index'] =~ /#{Regexp.escape(Time.now.strftime('%Y%m'))}/
        }
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
           '_id'          => 'just_enough',
           'published_at' => Time.now.months_ago(6).beginning_of_month.to_s }]
      end

      subject { klass.mention.save_document(docs: data) }

      it 'should has save only 1 document' do
        expect(subject).to have_requested(:post, /_bulk/).with { |request|
          request.body.split("\n").size == 2
        }
      end
    end
  end

  describe 'save to specific language' do
    let(:url) { 'localhost:9203' }
    before do
      ES[:disputatio][:mention_language_index] = true
      ES[:disputatio][:languages_write] = ['cs']
      ES[:disputatio][:mention_write_date] = true
      ES[:disputatio][:mention_max_age] = 6
      ES[:disputatio][:connect_another] = [
        { connect: { host: url }, mention_language_index: false }]
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
        response = ElasticsearchRasi::JsonHelper.load(request.body.split("\n")[0])
        response['index']['_index'] =~ /cs/
      }
    end
  end

  describe 'save to specific language only on another' do
    let(:url) { 'localhost:9203' }
    before do
      ES[:disputatio][:mention_language_index] = false
      ES[:disputatio][:mention_write_date] = true
      ES[:disputatio][:mention_read_date] = true
      ES[:disputatio][:mention_max_age] = 6
      ES[:disputatio][:connect_another] = [
        { connect: { host: url, log: true },
          mention_language_index: true, languages_write: ['cs'] }]
    end

    subject do
      klass.mention.save_document(docs:
        [
          { '_id' => 'test',
            'languages' => ['cs'],
            'author'    => 'author name',
            'published_at' => Time.now.strftime('%Y-%m-%d') }])
    end

    it 'should has save documents without language' do
      expect(subject).to have_requested(:post, /:9202\S+_bulk/).with { |request|
        response = ElasticsearchRasi::JsonHelper.load(request.body.split("\n")[0])
        response['index']['_index'] !~ /cs/
      }
    end

    it 'should has save documents with language' do
      expect(subject).to have_requested(:post, /:9203\S+_bulk/).with { |request|
        response = ElasticsearchRasi::JsonHelper.load(request.body.split("\n")[0])
        response['index']['_index'] =~ /cs/
      }
    end
  end
end
