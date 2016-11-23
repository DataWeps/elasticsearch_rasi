# encoding:utf-8
require 'active_support/core_ext/object/deep_dup'

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

    def send_request(method, params)
      counter = 0
      begin
        clone_params = params.deep_dup if another_es?(method)
        response = @es.send(method, params)
        return response unless another_es?(method)
        @es_another.each { |es| es.send(method.to_sym, clone_params) }
        return response
      rescue Elasticsearch::Transport::Transport::Errors::InternalServerError => e
        return { error: e.message }
      rescue Elasticsearch::Transport::Transport::Errors::BadRequest => e
        return { error: e.message }
      rescue Faraday::ConnectionFailed, Faraday::TimeoutError => e
        counter += 1
        if counter < ES[:connect_attempts]
          sleep ES[:connect_sleep]
          retry
        end
      end
      false
    end
  end
end
