# encoding:utf-8
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

    def send_request(method, params)
      counter = 0
      begin
        return @es.send(method, params)
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
