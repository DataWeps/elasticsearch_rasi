# encoding:utf-8
require 'active_support/core_ext/object/deep_dup'
require 'unicode_utils/downcase'
require 'digest/sha1'

class ElasticsearchRasi
  module Request
    CONTENT_TYPE = { content_type: 'application/json' }

    def request(method, params)
      send_request(method, params)
    end

    def clear_request(method, params)
      response = send_request(method, params)
      return false if response.include?(:error)
      response
    end

  private

    def another_es?(method)
      !@es_another.empty? &&
        !@config[:another_methods].blank? && @config[:another_methods].include?(method)
    end

    def compute_author_hash(author)
      UnicodeUtils.downcase(Digest::SHA1.hexdigest(author))
    end

    def prepare_params!(params)
      return true if @config[:verboom_bulk].blank?
      params[:body].each do |in_data|
        data = in_data.values[0]
        next if data.blank? || data[:data].blank?
        next if data[:data]['author'].blank?

        author =
          if data[:data]['author'].is_a?(Hash)
            data[:data]['author']['name']
          else
            data[:data]['author']
          end

        data[:data]['search_author'] = {
          'name' => author,
          'author_hash' => compute_author_hash(author) }
      end
    end

    def send_request(method, params)
      counter = 0
      begin
        clone_params = params.deep_dup if another_es?(method)
        response = @es.send(method, params)
        return response unless another_es?(method)
        prepare_params!(clone_params) if method.to_sym == :bulk
        @es_another.each { |es| es.send(method.to_sym, clone_params) }
        return response
      rescue Elasticsearch::Transport::Transport::Errors::InternalServerError => e
        return { error: e.message }
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        return { error: e.message }
      rescue Faraday::ConnectionFailed, Faraday::TimeoutError => e
        counter += 1
        if counter < ES[:connect_attempts]
          sleep(ES[:connect_sleep])
          retry
        end
      end
      false
    end
  end
end
